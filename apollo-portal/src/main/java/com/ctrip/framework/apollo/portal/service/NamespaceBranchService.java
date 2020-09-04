package com.ctrip.framework.apollo.portal.service;

import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleDTO;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.dto.ReleaseDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.portal.api.AdminServiceAPI;
import com.ctrip.framework.apollo.portal.component.ItemsComparator;
import com.ctrip.framework.apollo.portal.constant.TracerEventType;
import com.ctrip.framework.apollo.portal.entity.bo.NamespaceBO;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import com.ctrip.framework.apollo.tracer.Tracer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;

@Service
public class NamespaceBranchService {

  private final ItemsComparator itemsComparator;
  private final UserInfoHolder userInfoHolder;
  private final NamespaceService namespaceService;
  private final ItemService itemService;
  private final AdminServiceAPI.NamespaceBranchAPI namespaceBranchAPI;
  private final ReleaseService releaseService;

  public NamespaceBranchService(
      final ItemsComparator itemsComparator,
      final UserInfoHolder userInfoHolder,
      final NamespaceService namespaceService,
      final ItemService itemService,
      final AdminServiceAPI.NamespaceBranchAPI namespaceBranchAPI,
      final ReleaseService releaseService) {
    this.itemsComparator = itemsComparator;
    this.userInfoHolder = userInfoHolder;
    this.namespaceService = namespaceService;
    this.itemService = itemService;
    this.namespaceBranchAPI = namespaceBranchAPI;
    this.releaseService = releaseService;
  }


  @Transactional
  public NamespaceDTO createBranch(String appId, Env env, String parentClusterName, String namespaceName) {
    String operator = userInfoHolder.getUser().getUserId();
    return createBranch(appId, env, parentClusterName, namespaceName, operator);
  }


  /**
   * 创建分支
   * @param appId
   * @param env
   * @param parentClusterName
   * @param namespaceName
   * @param operator
   * @return
   */
  @Transactional
  public NamespaceDTO createBranch(String appId, Env env, String parentClusterName, String namespaceName, String operator) {
    //调用Admin Service接口创建分支
    NamespaceDTO createdBranch = namespaceBranchAPI.createBranch(appId, env, parentClusterName, namespaceName,
            operator);

    Tracer.logEvent(TracerEventType.CREATE_GRAY_RELEASE, String.format("%s+%s+%s+%s", appId, env, parentClusterName,
            namespaceName));
    return createdBranch;

  }

  public GrayReleaseRuleDTO findBranchGrayRules(String appId, Env env, String clusterName,
                                                String namespaceName, String branchName) {
    return namespaceBranchAPI.findBranchGrayRules(appId, env, clusterName, namespaceName, branchName);

  }

  /**
   * 更新namespace分支的灰度规则
   * @param appId
   * @param env
   * @param clusterName
   * @param namespaceName
   * @param branchName
   * @param rules
   */
  public void updateBranchGrayRules(String appId, Env env, String clusterName, String namespaceName,
                                    String branchName, GrayReleaseRuleDTO rules) {

    String operator = userInfoHolder.getUser().getUserId();
    updateBranchGrayRules(appId, env, clusterName, namespaceName, branchName, rules, operator);
  }

  public void updateBranchGrayRules(String appId, Env env, String clusterName, String namespaceName,
                                    String branchName, GrayReleaseRuleDTO rules, String operator) {
    rules.setDataChangeCreatedBy(operator);
    rules.setDataChangeLastModifiedBy(operator);

    //调用接口 更新namespace分支的灰度规则
    namespaceBranchAPI.updateBranchGrayRules(appId, env, clusterName, namespaceName, branchName, rules);

    Tracer.logEvent(TracerEventType.UPDATE_GRAY_RELEASE_RULE,
            String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));
  }

  public void deleteBranch(String appId, Env env, String clusterName, String namespaceName,
                           String branchName) {

    String operator = userInfoHolder.getUser().getUserId();
    deleteBranch(appId, env, clusterName, namespaceName, branchName, operator);
  }

  public void deleteBranch(String appId, Env env, String clusterName, String namespaceName,
                           String branchName, String operator) {
    namespaceBranchAPI.deleteBranch(appId, env, clusterName, namespaceName, branchName, operator);

    Tracer.logEvent(TracerEventType.DELETE_GRAY_RELEASE,
            String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));
  }

  /**
   * 调用Admin Service Api 合并子namespace变更的配置Map到父namespace
   * 并进行一次release
   * @param appId
   * @param env
   * @param clusterName
   * @param namespaceName
   * @param branchName
   * @param title
   * @param comment
   * @param isEmergencyPublish
   * @param deleteBranch
   * @return
   */
  public ReleaseDTO merge(String appId, Env env, String clusterName, String namespaceName,
                          String branchName, String title, String comment,
                          boolean isEmergencyPublish, boolean deleteBranch) {
    String operator = userInfoHolder.getUser().getUserId();
    return merge(appId, env, clusterName, namespaceName, branchName, title, comment, isEmergencyPublish, deleteBranch, operator);
  }

  public ReleaseDTO merge(String appId, Env env, String clusterName, String namespaceName,
                          String branchName, String title, String comment,
                          boolean isEmergencyPublish, boolean deleteBranch, String operator) {

    //计算变化的item集合
    ItemChangeSets changeSets = calculateBranchChangeSet(appId, env, clusterName, namespaceName, branchName, operator);

    //合并子namespace变更的配置Map到父namespace，并进行一次release
    ReleaseDTO mergedResult =
            releaseService.updateAndPublish(appId, env, clusterName, namespaceName, title, comment,
                    branchName, isEmergencyPublish, deleteBranch, changeSets);

    Tracer.logEvent(TracerEventType.MERGE_GRAY_RELEASE,
            String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));

    return mergedResult;
  }

  /**
   * 计算变化的item集合
   * @param appId
   * @param env
   * @param clusterName
   * @param namespaceName
   * @param branchName
   * @param operator
   * @return
   */
  private ItemChangeSets calculateBranchChangeSet(String appId, Env env, String clusterName, String namespaceName,
                                                  String branchName, String operator) {
    //apollo-portal侧用 BO 获取父namespaceBO对象
    NamespaceBO parentNamespace = namespaceService.loadNamespaceBO(appId, env, clusterName, namespaceName);

    if (parentNamespace == null) {
      throw new BadRequestException("base namespace not existed");
    }

    //若父namespace有配置项的变更，不允许合并，因为可能存在冲突
    if (parentNamespace.getItemModifiedCnt() > 0) {
      throw new BadRequestException("Merge operation failed. Because master has modified items");
    }

    //获得父namespace的item数组
    List<ItemDTO> masterItems = itemService.findItems(appId, env, clusterName, namespaceName);

    //获得子namespace的item数组
    List<ItemDTO> branchItems = itemService.findItems(appId, env, branchName, namespaceName);

    //计算变化的item集合
    ItemChangeSets changeSets = itemsComparator.compareIgnoreBlankAndCommentItem(parentNamespace.getBaseInfo().getId(),
            masterItems, branchItems);
    //设置 `ItemChangeSets.deleteItem` 为空。因为子 Namespace 从父 Namespace 继承配置，
    // 但是实际自己没有那些配置项，所以如果不清空，会导致这些配置项被删除
    changeSets.setDeleteItems(Collections.emptyList());
    //设置 `ItemChangeSets.dataChangeLastModifiedBy` 为当前管理员
    changeSets.setDataChangeLastModifiedBy(operator);
    return changeSets;
  }

  public NamespaceDTO findBranchBaseInfo(String appId, Env env, String clusterName, String namespaceName) {
    return namespaceBranchAPI.findBranch(appId, env, clusterName, namespaceName);
  }

  public NamespaceBO findBranch(String appId, Env env, String clusterName, String namespaceName) {
    NamespaceDTO namespaceDTO = findBranchBaseInfo(appId, env, clusterName, namespaceName);
    if (namespaceDTO == null) {
      return null;
    }
    return namespaceService.loadNamespaceBO(appId, env, namespaceDTO.getClusterName(), namespaceName);
  }

}
