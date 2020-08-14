package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 * Admin Service在配置发布后，需要通知所有的Config Service有配置发布，
 * 从而Config Service可以通知对应的客户端来拉取最新的配置
 * Admin Service 为producer发出消息，各个Config Service作为consumer 消费消息
 * 考虑到Apollo的实际使用场景，以及为了尽可能减少外部依赖，我们没有采用外部的消息中间件，
 * 而是通过数据库实现了一个简单的消息队列
 * Admin Service在配置发布后会往ReleaseMessage表插入一条消息记录，消息内容就是配置发布的AppId+Cluster+Namespace
 */
@Component
public class DatabaseMessageSender implements MessageSender {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
  /**
   * 清理message 队列，最大容量
   */
  private static final int CLEAN_QUEUE_MAX_SIZE = 100;
  /**
   * 清理message队列
   */
  private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
  /**
   * 清理message ExecutorService
   */
  private final ExecutorService cleanExecutorService;
  /**
   * 是否停止清理 Message标识
   */
  private final AtomicBoolean cleanStopped;

  private final ReleaseMessageRepository releaseMessageRepository;

  public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
    // 创建 ExecutorService 对象
    cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
    // 设置 cleanStopped 为 false
    cleanStopped = new AtomicBoolean(false);
    this.releaseMessageRepository = releaseMessageRepository;
  }

  @Override
  @Transactional
  public void sendMessage(String message, String channel) {
    logger.info("Sending message {} to channel {}", message, channel);
    // 仅允许发送 APOLLO_RELEASE_TOPIC
    if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {//消息主题为 apollo-release
      logger.warn("Channel {} not supported by DatabaseMessageSender!", channel);
      return;
    }

    Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
    Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
    try {
      // 保存 ReleaseMessage 对象
      //Admin Service发布配置后，往releaseMessage表插入一条消息记录
      ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
      // 添加到清理 Message 队列。若队列已满，添加失败，不阻塞等待。
      toClean.offer(newMessage.getId());//加id加入队列
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      logger.error("Sending message to database failed", ex);
      transaction.setStatus(ex);
      throw ex;
    } finally {
      transaction.complete();
    }
  }

  /**
   * 通知spring调用，初始化清理releasemessage任务
   */
  @PostConstruct
  private void initialize() {
    cleanExecutorService.submit(() -> {
      // 若未停止，持续运行
      while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // 拉取
          Long rm = toClean.poll(1, TimeUnit.SECONDS);
          // 队列非空，处理拉取到的消息
          if (rm != null) {
            cleanMessage(rm);
          } else {
            // 队列为空，sleep ，避免空跑，占用 CPU
            TimeUnit.SECONDS.sleep(5);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  private void cleanMessage(Long id) {
    // 查询对应的 ReleaseMessage 对象，避免已经删除。因为，DatabaseMessageSender 会在多进程中执行。
    // 例如：1）Config Service + Admin Service ；2）N * Config Service ；3）N * Admin Service
    //double check in case the release message is rolled back
    ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
    if (releaseMessage == null) {
      return;
    }
    boolean hasMore = true;
    // 循环删除相同消息内容( `message` )的老消息
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      // 拉取相同消息内容的 100 条的老消息
      // 老消息的定义：比当前消息编号小，即先发送的
      // 按照 id 升序
      List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
          releaseMessage.getMessage(), releaseMessage.getId());
      // 删除老消息
      releaseMessageRepository.deleteAll(messages);
      hasMore = messages.size() == 100;

      messages.forEach(toRemove -> Tracer.logEvent(
          String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
    }
  }

  void stopClean() {
    cleanStopped.set(true);
  }
}
