package com.ctrip.framework.apollo.biz.message;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 * Config Service有一个线程会每秒扫描一次ReleaseMessage表，看看是否有新的消息记录
 * Config Service如果发现有新的消息记录，那么就会通知到所有的消息监听器（ReleaseMessageListener），
 * 如NotificationControllerV2，消息监听器的注册过程参见ConfigServiceAutoConfiguration
 * NotificationControllerV2得到配置发布的AppId+Cluster+Namespace后，会通知对应的客户端
 */
public class ReleaseMessageScanner implements InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageScanner.class);
  @Autowired
  private BizConfig bizConfig;
  @Autowired
  private ReleaseMessageRepository releaseMessageRepository;
  /**
   * 从 DB 中扫描 ReleaseMessage 表的频率，单位：毫秒
   */
  private int databaseScanInterval;
  //"发布消息"监听器 releasemessagelistener
  //监听器数组
  private List<ReleaseMessageListener> listeners;
  /**
   * 定时器任务服务
   */
  private ScheduledExecutorService executorService;
  /**
   * 最后扫描到的ReleaseMessage的编号
   */
  private long maxIdScanned;

  public ReleaseMessageScanner() {
    // 创建监听器数组
    listeners = Lists.newCopyOnWriteArrayList();
    // 创建 ScheduledExecutorService 对象
    executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("ReleaseMessageScanner", true));
  }

  /**
   * 通过spring调用，初始化scan任务
   * @throws Exception
   */
  @Override
  public void afterPropertiesSet() throws Exception {
    // 从 ServerConfig 中获得频率
    databaseScanInterval = bizConfig.releaseMessageScanIntervalInMilli();
    // 获得最大的 ReleaseMessage 的编号
    maxIdScanned = loadLargestMessageId();
    // 创建从 DB 中扫描 ReleaseMessage 表的定时任务
    executorService.scheduleWithFixedDelay((Runnable) () -> {
      Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageScanner", "scanMessage");
      try {
        // 从 DB 中，扫描 ReleaseMessage 们
        scanMessages();
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        transaction.setStatus(ex);
        logger.error("Scan and send message failed", ex);
      } finally {
        transaction.complete();
      }
    }, databaseScanInterval, databaseScanInterval, TimeUnit.MILLISECONDS);

  }

  /**
   * add message listeners for release message
   * @param listener
   */
  public void addMessageListener(ReleaseMessageListener listener) {
    if (!listeners.contains(listener)) {
      listeners.add(listener);
    }
  }

  /**
   * Scan messages, continue scanning until there is no more messages
   */
  private void scanMessages() {
    boolean hasMoreMessages = true;
    while (hasMoreMessages && !Thread.currentThread().isInterrupted()) {
      hasMoreMessages = scanAndSendMessages();
    }
  }

  /**
   * scan messages and send
   * 扫描消息 并发送
   * @return whether there are more messages
   */
  private boolean scanAndSendMessages() {
    // 获得大于 maxIdScanned 的 500 条 ReleaseMessage 记录，按照 id 升序
    //current batch is 500
    List<ReleaseMessage> releaseMessages =
        releaseMessageRepository.findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
    if (CollectionUtils.isEmpty(releaseMessages)) {
      return false;
    }
    // 触发监听器
    fireMessageScanned(releaseMessages);
    // 获得新的 maxIdScanned ，取最后一条记录
    int messageScanned = releaseMessages.size();
    maxIdScanned = releaseMessages.get(messageScanned - 1).getId();
    // 若拉取不足 500 条，说明无新消息了
    return messageScanned == 500;
  }

  /**
   * find largest message id as the current start point
   * @return current largest message id
   */
  private long loadLargestMessageId() {
    ReleaseMessage releaseMessage = releaseMessageRepository.findTopByOrderByIdDesc();
    return releaseMessage == null ? 0 : releaseMessage.getId();
  }

  /**
   * Notify listeners with messages loaded
   * 发现有新的消息记录，则通知所有的消息监听器
   * @param messages
   */
  private void fireMessageScanned(List<ReleaseMessage> messages) {
    for (ReleaseMessage message : messages) {
      for (ReleaseMessageListener listener : listeners) {
        try {
          //消息主题为 apollo-release
          listener.handleMessage(message, Topics.APOLLO_RELEASE_TOPIC);
        } catch (Throwable ex) {
          Tracer.logError(ex);
          logger.error("Failed to invoke message listener {}", listener.getClass(), ex);
        }
      }
    }
  }
}
