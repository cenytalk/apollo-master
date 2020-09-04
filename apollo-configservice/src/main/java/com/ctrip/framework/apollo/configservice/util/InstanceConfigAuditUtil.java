package com.ctrip.framework.apollo.configservice.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import com.ctrip.framework.apollo.biz.entity.Instance;
import com.ctrip.framework.apollo.biz.entity.InstanceConfig;
import com.ctrip.framework.apollo.biz.service.InstanceService;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 实现 InitializingBean 接口，InstanceConfig 审计工具类
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class InstanceConfigAuditUtil implements InitializingBean {
  /**
   * {@link #audits} 大小
   */
  private static final int INSTANCE_CONFIG_AUDIT_MAX_SIZE = 10000;
  /**
   * {@link #instanceCache} 大小
   */
  private static final int INSTANCE_CACHE_MAX_SIZE = 50000;
  /**
   * {@link #instanceConfigReleaseKeyCache} 大小
   */
  private static final int INSTANCE_CONFIG_CACHE_MAX_SIZE = 50000;
  private static final long OFFER_TIME_LAST_MODIFIED_TIME_THRESHOLD_IN_MILLI = TimeUnit.MINUTES.toMillis(10);//10 minutes
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  /**
   * ExecutorService 对象。队列大小为 1
   */
  private final ExecutorService auditExecutorService;
  /**
   * 是否停止
   */
  private final AtomicBoolean auditStopped;
  /**
   * 队列
   */
  private BlockingQueue<InstanceConfigAuditModel> audits = Queues.newLinkedBlockingQueue
      (INSTANCE_CONFIG_AUDIT_MAX_SIZE);
  /**
   * Instance 的编号的缓存
   *  KEY ，使用 appId + clusterName + dataCenter + ip ，恰好是 Instance 的唯一索引的字段
   *
   * KEY：{@link #assembleInstanceKey(String, String, String, String)}
   * VALUE：{@link Instance#id}
   */
  private Cache<String, Long> instanceCache;
  /**
   * InstanceConfig 的 ReleaseKey 的缓存
   * KEY ，使用 instanceId + configAppId + ConfigNamespaceName ，恰好是 InstanceConfig 的唯一索引的字段
   *
   * KEY：{@link #assembleInstanceConfigKey(long, String, String)}
   * VALUE：{@link InstanceConfig#id}
   */
  private Cache<String, String> instanceConfigReleaseKeyCache;

  private final InstanceService instanceService;

  public InstanceConfigAuditUtil(final InstanceService instanceService) {
    this.instanceService = instanceService;
    auditExecutorService = Executors.newSingleThreadExecutor(
        ApolloThreadFactory.create("InstanceConfigAuditUtil", true));
    auditStopped = new AtomicBoolean(false);
    instanceCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(INSTANCE_CACHE_MAX_SIZE).build();
    instanceConfigReleaseKeyCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.DAYS)
        .maximumSize(INSTANCE_CONFIG_CACHE_MAX_SIZE).build();
  }

  /**
   *  添加到队列中
   *
   * @param appId
   * @param clusterName
   * @param dataCenter
   * @param ip
   * @param configAppId
   * @param configClusterName
   * @param configNamespace
   * @param releaseKey
   * @return
   */
  public boolean audit(String appId, String clusterName, String dataCenter, String
      ip, String configAppId, String configClusterName, String configNamespace, String releaseKey) {
    return this.audits.offer(new InstanceConfigAuditModel(appId, clusterName, dataCenter, ip,
        configAppId, configClusterName, configNamespace, releaseKey));//audits为队列 往队列中加入数据
  }

  /**
   * 记录Instance(使用配置的应用实例)
   * 记录IntanceConfig(应用实例的配置信息)
   * @param auditModel
   */
  void doAudit(InstanceConfigAuditModel auditModel) {
    //拼接instanceCache的key appid+clustername+ip+datacenter 确定instance的唯一索引
    String instanceCacheKey = assembleInstanceKey(auditModel.getAppId(), auditModel
        .getClusterName(), auditModel.getIp(), auditModel.getDataCenter());
    //获得instance编号
    Long instanceId = instanceCache.getIfPresent(instanceCacheKey);
    //如果没有查询到，则从DB中加载或者新创建instance保存到db中，然后添加到缓存中
    if (instanceId == null) {
      instanceId = prepareInstanceId(auditModel);
      instanceCache.put(instanceCacheKey, instanceId);
    }

    //拼接instanceConfigReleaseKeyCache的key,该key为instanceid+configappid+confignamespacename,即为instanceconfig表的唯一索引
    //load instance config release key from cache, and check if release key is the same
    String instanceConfigCacheKey = assembleInstanceConfigKey(instanceId, auditModel
        .getConfigAppId(), auditModel.getConfigNamespace());
    //获得缓存的releasekey
    String cacheReleaseKey = instanceConfigReleaseKeyCache.getIfPresent(instanceConfigCacheKey);

    //若相等，则跳过，为什么相等就跳过呢？
    //if release key is the same, then skip audit
    if (cacheReleaseKey != null && Objects.equals(cacheReleaseKey, auditModel.getReleaseKey())) {
      return;
    }

    //更新对应的instanceConfigReleaseKeyCache
    instanceConfigReleaseKeyCache.put(instanceConfigCacheKey, auditModel.getReleaseKey());

    //从数据库中获得InstanceConfig
    //if release key is not the same or cannot find in cache, then do audit
    InstanceConfig instanceConfig = instanceService.findInstanceConfig(instanceId, auditModel
        .getConfigAppId(), auditModel.getConfigNamespace());

    //如果db中已经存在该Instanceconfig ，则对其进行更新
    if (instanceConfig != null) {
      //releasekey发生变化
      if (!Objects.equals(instanceConfig.getReleaseKey(), auditModel.getReleaseKey())) {
        instanceConfig.setConfigClusterName(auditModel.getConfigClusterName());
        instanceConfig.setReleaseKey(auditModel.getReleaseKey());
        instanceConfig.setReleaseDeliveryTime(auditModel.getOfferTime());
      } else if (offerTimeAndLastModifiedTimeCloseEnough(auditModel.getOfferTime(),
          instanceConfig.getDataChangeLastModifiedTime())) {
        // 时间过近，例如 Client 先请求的 Config Service A 节点，再请求 Config Service B 节点的情况，则减少更新
        //when releaseKey is the same, optimize to reduce writes if the record was updated not long ago
        return;
      }
      //更新
      //we need to update no matter the release key is the same or not, to ensure the
      //last modified time is updated each day
      instanceConfig.setDataChangeLastModifiedTime(auditModel.getOfferTime());
      instanceService.updateInstanceConfig(instanceConfig);
      return;
    }

    //如果db中不存在该instanceconfig，则新建instanceconfig,并保存在db中
    instanceConfig = new InstanceConfig();
    instanceConfig.setInstanceId(instanceId);
    instanceConfig.setConfigAppId(auditModel.getConfigAppId());
    instanceConfig.setConfigClusterName(auditModel.getConfigClusterName());
    instanceConfig.setConfigNamespaceName(auditModel.getConfigNamespace());
    instanceConfig.setReleaseKey(auditModel.getReleaseKey());
    instanceConfig.setReleaseDeliveryTime(auditModel.getOfferTime());
    instanceConfig.setDataChangeCreatedTime(auditModel.getOfferTime());

    try {
      instanceService.createInstanceConfig(instanceConfig);
    } catch (DataIntegrityViolationException ex) {
      //concurrent insertion, safe to ignore
    }
  }

  /**
   * 时间过近，仅相差 10 分钟。例如，Client 先请求的 Config Service A 节点，再请求 Config Service B 节点的情况。
   * 此时，InstanceConfig 在 DB 中是已经更新了，但是在 Config Service B 节点的缓存是未更新的
   * @param offerTime
   * @param lastModifiedTime
   * @return
   */
  private boolean offerTimeAndLastModifiedTimeCloseEnough(Date offerTime, Date lastModifiedTime) {
    return (offerTime.getTime() - lastModifiedTime.getTime()) <
        OFFER_TIME_LAST_MODIFIED_TIME_THRESHOLD_IN_MILLI;
  }

  /**
   * 预备获得应用实例的in，即instanceid
   * 通过InstanceConfigAuditModel获得instance,如果db中存在改instance，则直接取的instanceid
   * 如果db中不存在该instance，则创建新的instance，保存在db中，并返回instanceid
   * @param auditModel
   * @return
   */
  private long prepareInstanceId(InstanceConfigAuditModel auditModel) {
    //从db中查询instance
    Instance instance = instanceService.findInstance(auditModel.getAppId(), auditModel
        .getClusterName(), auditModel.getDataCenter(), auditModel.getIp());
    //如果存在，则直接返回instanceid
    if (instance != null) {
      return instance.getId();
    }
    //如果不存在，则创建新的instance，保存在db中，并返回instanceid
    instance = new Instance();
    instance.setAppId(auditModel.getAppId());
    instance.setClusterName(auditModel.getClusterName());
    instance.setDataCenter(auditModel.getDataCenter());
    instance.setIp(auditModel.getIp());


    try {
      return instanceService.createInstance(instance).getId();
    } catch (DataIntegrityViolationException ex) {
      //因为instance中存在通过appid+clustername+ip+datacenter的唯一索引
      //抛出异常，则意味着发生唯一索引冲突，意味db中已经存在instance,则直接从db中查询并返回instanceid
      //return the one exists
      return instanceService.findInstance(instance.getAppId(), instance.getClusterName(),
          instance.getDataCenter(), instance.getIp()).getId();
    }
  }

  /**
   * 通过spring调用，初始化任务
   * @throws Exception
   */
  @Override
  public void afterPropertiesSet() throws Exception {
    //提交任务
    auditExecutorService.submit(() -> {
      //循环，直到停止或线程打断
      while (!auditStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          //获得队列首元素 InstanceConfigAuditModel,非阻塞
          InstanceConfigAuditModel model = audits.poll();
          //若获取不到，sleep 等待1秒
          if (model == null) {
            TimeUnit.SECONDS.sleep(1);
            continue;
          }
          //若获取到，记入Instance和InstanceConfig
          doAudit(model);
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  private String assembleInstanceKey(String appId, String cluster, String ip, String datacenter) {
    List<String> keyParts = Lists.newArrayList(appId, cluster, ip);
    if (!Strings.isNullOrEmpty(datacenter)) {
      keyParts.add(datacenter);
    }
    return STRING_JOINER.join(keyParts);
  }

  private String assembleInstanceConfigKey(long instanceId, String configAppId, String configNamespace) {
    return STRING_JOINER.join(instanceId, configAppId, configNamespace);
  }

  /**
   * 内部静态类
   */
  public static class InstanceConfigAuditModel {
    private String appId;
    private String clusterName;
    private String dataCenter;
    private String ip;
    private String configAppId;
    private String configClusterName;
    private String configNamespace;
    private String releaseKey;
    //入队时间
    private Date offerTime;

    public InstanceConfigAuditModel(String appId, String clusterName, String dataCenter, String
        clientIp, String configAppId, String configClusterName, String configNamespace, String
                                        releaseKey) {
      this.offerTime = new Date();
      this.appId = appId;
      this.clusterName = clusterName;
      this.dataCenter = Strings.isNullOrEmpty(dataCenter) ? "" : dataCenter;
      this.ip = clientIp;
      this.configAppId = configAppId;
      this.configClusterName = configClusterName;
      this.configNamespace = configNamespace;
      this.releaseKey = releaseKey;
    }

    public String getAppId() {
      return appId;
    }

    public String getClusterName() {
      return clusterName;
    }

    public String getDataCenter() {
      return dataCenter;
    }

    public String getIp() {
      return ip;
    }

    public String getConfigAppId() {
      return configAppId;
    }

    public String getConfigNamespace() {
      return configNamespace;
    }

    public String getReleaseKey() {
      return releaseKey;
    }

    public String getConfigClusterName() {
      return configClusterName;
    }

    public Date getOfferTime() {
      return offerTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
          return true;
      }
      if (o == null || getClass() != o.getClass()) {
          return false;
      }
      InstanceConfigAuditModel model = (InstanceConfigAuditModel) o;
      return Objects.equals(appId, model.appId) &&
          Objects.equals(clusterName, model.clusterName) &&
          Objects.equals(dataCenter, model.dataCenter) &&
          Objects.equals(ip, model.ip) &&
          Objects.equals(configAppId, model.configAppId) &&
          Objects.equals(configClusterName, model.configClusterName) &&
          Objects.equals(configNamespace, model.configNamespace) &&
          Objects.equals(releaseKey, model.releaseKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, clusterName, dataCenter, ip, configAppId, configClusterName,
          configNamespace,
          releaseKey);
    }
  }
}
