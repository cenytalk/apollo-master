package com.ctrip.framework.apollo.configservice.util;

import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jason Song(song_s@ctrip.com)
 * Watch Key 工具类
 */
@Component
public class WatchKeysUtil {
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private final AppNamespaceServiceWithCache appNamespaceService;

  public WatchKeysUtil(final AppNamespaceServiceWithCache appNamespaceService) {
    this.appNamespaceService = appNamespaceService;
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespace, dataCenter combination
   */
  public Set<String> assembleAllWatchKeys(String appId, String clusterName, String namespace,
                                          String dataCenter) {
    Multimap<String, String> watchedKeysMap =
        assembleAllWatchKeys(appId, clusterName, Sets.newHashSet(namespace), dataCenter);
    return Sets.newHashSet(watchedKeysMap.get(namespace));
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespaces, dataCenter combination
   * 组装所有的 Watch Key Multimap 。其中 Key 为 Namespace 的名字，Value 为 Watch Key 集合
   *
   * @return a multimap with namespace as the key and watch keys as the value
   */
  public Multimap<String, String> assembleAllWatchKeys(String appId, String clusterName,
                                                       Set<String> namespaces,
                                                       String dataCenter) {
    // 组装 Watch Key Multimap
    Multimap<String, String> watchedKeysMap =
        assembleWatchKeys(appId, clusterName, namespaces, dataCenter);

    // 如果不是仅监听 'application' Namespace ，处理其关联来的 Namespace
    //Every app has an 'application' namespace
    if (!(namespaces.size() == 1 && namespaces.contains(ConfigConsts.NAMESPACE_APPLICATION))) {
      // 获得属于该 App 的 Namespace 的名字的集合
      Set<String> namespacesBelongToAppId = namespacesBelongToAppId(appId, namespaces);
      // 获得关联来的 Namespace 的名字的集合
      Set<String> publicNamespaces = Sets.difference(namespaces, namespacesBelongToAppId);
      // 添加到 Watch Key Multimap 中
      //Listen on more namespaces if it's a public namespace
      if (!publicNamespaces.isEmpty()) {
        watchedKeysMap
            .putAll(findPublicConfigWatchKeys(appId, clusterName, publicNamespaces, dataCenter));
      }
    }

    return watchedKeysMap;
  }

  /**
   * 获得 Namespace 类型为 public 对应的 Watch Key Multimap
   * 重要：要求非当前 App 的 Namespace
   * @param applicationId
   * @param clusterName
   * @param namespaces
   * @param dataCenter
   * @return
   */
  private Multimap<String, String> findPublicConfigWatchKeys(String applicationId,
                                                             String clusterName,
                                                             Set<String> namespaces,
                                                             String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();
    // 获得 Namespace 为 public 的 AppNamespace 数组
    List<AppNamespace> appNamespaces = appNamespaceService.findPublicNamespacesByNames(namespaces);

    // 组装 Watch Key Map
    for (AppNamespace appNamespace : appNamespaces) {
      // 排除非关联类型的 Namespace
      //check whether the namespace's appId equals to current one
      if (Objects.equals(applicationId, appNamespace.getAppId())) {
        continue;
      }

      String publicConfigAppId = appNamespace.getAppId();

      // 组装指定 Namespace 的 Watch Key 数组
      watchedKeysMap.putAll(appNamespace.getName(),
          assembleWatchKeys(publicConfigAppId, clusterName, appNamespace.getName(), dataCenter));
    }

    return watchedKeysMap;
  }

  /**
   * 拼接 Watch Key appId+cluster+namespace
   * Watch Key 的格式和 ReleaseMessage.message 的格式是一致的
   * @param appId
   * @param cluster
   * @param namespace
   * @return
   */
  private String assembleKey(String appId, String cluster, String namespace) {
    return STRING_JOINER.join(appId, cluster, namespace);
  }

  /**
   * 组装指定 Namespace 的 Watch Key 数组
   * @param appId
   * @param clusterName
   * @param namespace
   * @param dataCenter
   * @return
   */
  private Set<String> assembleWatchKeys(String appId, String clusterName, String namespace,
                                        String dataCenter) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    Set<String> watchedKeys = Sets.newHashSet();

    // 指定 Cluster
    //watch specified cluster config change
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, clusterName)) {//default
      watchedKeys.add(assembleKey(appId, clusterName, namespace));
    }

    // 所属 IDC 的 Cluster
    // https://github.com/ctripcorp/apollo/issues/952
    //watch data center config change
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, clusterName)) {
      watchedKeys.add(assembleKey(appId, dataCenter, namespace));
    }

    // 默认 Cluster
    //watch default cluster config change
    watchedKeys.add(assembleKey(appId, ConfigConsts.CLUSTER_NAME_DEFAULT, namespace));

    return watchedKeys;
  }

  /**
   * 组装 Watch Key Multimap
   * @param appId App 编号
   * @param clusterName Cluster 名
   * @param namespaces  Namespace 的名字的集合
   * @param dataCenter  IDC 的 Cluster 名字
   * @return  Watch Key Multimap
   */
  private Multimap<String, String> assembleWatchKeys(String appId, String clusterName,
                                                     Set<String> namespaces,
                                                     String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();

    // 循环 Namespace 的名字的集合
    for (String namespace : namespaces) {
      watchedKeysMap
          .putAll(namespace, assembleWatchKeys(appId, clusterName, namespace, dataCenter));
    }

    return watchedKeysMap;
  }

  /**
   * 获得属于该 App 的 Namespace 的名字的集合
   * @param appId
   * @param namespaces
   * @return
   */
  private Set<String> namespacesBelongToAppId(String appId, Set<String> namespaces) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    // 获得属于该 App 的 AppNamespace 集合
    List<AppNamespace> appNamespaces =
        appNamespaceService.findByAppIdAndNamespaces(appId, namespaces);

    if (appNamespaces == null || appNamespaces.isEmpty()) {
      return Collections.emptySet();
    }

    /**
     * 返回 AppNamespace 的名字的集合
     * 当要传递给lambda体的操作，已经有实现的方法时，可以使用方法引用
     * 下面使用到了java8中 方法引用 使用操作符 "::"将方法名和对象或类的名字分隔开来
     * 主要有如下三种情况
     * 1、对象::实例方法
     * 2、类::实例方法
     * 3、类::实例方法
     */

    /**
     * Stream()
     * 1、stream()返回一个顺序流
     * 2、map(Function f) 接收一个函数作为参数，该函数被应用到每个元素上，并将其映射成一个新的元素
     * 3、collect(Collector c) 将流转换为其他形式，接收一个Collector接口的实现，用于给Stream中元素
     * 做汇总的方法
     */
    return appNamespaces.stream().map(AppNamespace::getName).collect(Collectors.toSet());//将流中元素搜集到set中
  }
}
