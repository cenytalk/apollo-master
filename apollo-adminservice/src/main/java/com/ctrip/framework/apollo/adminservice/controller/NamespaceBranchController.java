package com.ctrip.framework.apollo.adminservice.controller;

import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.message.MessageSender;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.NamespaceBranchService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NamespaceBranchController {

  private final MessageSender messageSender;
  private final NamespaceBranchService namespaceBranchService;
  private final NamespaceService namespaceService;

  public NamespaceBranchController(
      final MessageSender messageSender,
      final NamespaceBranchService namespaceBranchService,
      final NamespaceService namespaceService) {
    this.messageSender = messageSender;
    this.namespaceBranchService = namespaceBranchService;
    this.namespaceService = namespaceService;
  }


  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches")
  public NamespaceDTO createBranch(@PathVariable String appId,
                                   @PathVariable String clusterName,
                                   @PathVariable String namespaceName,
                                   @RequestParam("operator") String operator) {
    //检查namespace是否存在
    checkNamespace(appId, clusterName, namespaceName);

    //创建子namespace
    Namespace createdBranch = namespaceBranchService.createBranch(appId, clusterName, namespaceName, operator);

    //将namespace转换成namespaceDTO对象
    return BeanUtils.transform(NamespaceDTO.class, createdBranch);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}/rules")
  public GrayReleaseRuleDTO findBranchGrayRules(@PathVariable String appId,
                                                @PathVariable String clusterName,
                                                @PathVariable String namespaceName,
                                                @PathVariable String branchName) {

    checkBranch(appId, clusterName, namespaceName, branchName);

    GrayReleaseRule rules = namespaceBranchService.findBranchGrayRules(appId, clusterName, namespaceName, branchName);
    if (rules == null) {
      return null;
    }
    GrayReleaseRuleDTO ruleDTO =
        new GrayReleaseRuleDTO(rules.getAppId(), rules.getClusterName(), rules.getNamespaceName(),
                               rules.getBranchName());

    ruleDTO.setReleaseId(rules.getReleaseId());

    ruleDTO.setRuleItems(GrayReleaseRuleItemTransformer.batchTransformFromJSON(rules.getRules()));

    return ruleDTO;
  }

  /**
   * 更新namespace分支的灰度规则
   * @param appId
   * @param clusterName
   * @param namespaceName
   * @param branchName
   * @param newRuleDto
   */
  @Transactional
  @PutMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}/rules")
  public void updateBranchGrayRules(@PathVariable String appId, @PathVariable String clusterName,
                                    @PathVariable String namespaceName, @PathVariable String branchName,
                                    @RequestBody GrayReleaseRuleDTO newRuleDto) {

    //校验子namespace
    checkBranch(appId, clusterName, namespaceName, branchName);

    //将GrayReleaseRuleDTO对象转化为GrayReleaseRule对象（apollo-portal侧使用DTO）
    GrayReleaseRule newRules = BeanUtils.transform(GrayReleaseRule.class, newRuleDto);
    //JSON 化规则为字符串，并设置到GrayReleaseRule中
    newRules.setRules(GrayReleaseRuleItemTransformer.batchTransformToJSON(newRuleDto.getRuleItems()));
    //设置branchstatus为active
    newRules.setBranchStatus(NamespaceBranchStatus.ACTIVE);
    //更新子namespace的灰度发布规则
    namespaceBranchService.updateBranchGrayRules(appId, clusterName, namespaceName, branchName, newRules);

    //发送release消息 通知客户端更新配置
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
  }

  @Transactional
  @DeleteMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}")
  public void deleteBranch(@PathVariable String appId, @PathVariable String clusterName,
                           @PathVariable String namespaceName, @PathVariable String branchName,
                           @RequestParam("operator") String operator) {

    checkBranch(appId, clusterName, namespaceName, branchName);

    namespaceBranchService
        .deleteBranch(appId, clusterName, namespaceName, branchName, NamespaceBranchStatus.DELETED, operator);

    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);

  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches")
  public NamespaceDTO loadNamespaceBranch(@PathVariable String appId, @PathVariable String clusterName,
                                          @PathVariable String namespaceName) {

    checkNamespace(appId, clusterName, namespaceName);

    Namespace childNamespace = namespaceBranchService.findBranch(appId, clusterName, namespaceName);
    if (childNamespace == null) {
      return null;
    }

    return BeanUtils.transform(NamespaceDTO.class, childNamespace);
  }

  /**
   * 校验子namespace是否存在
   * @param appId
   * @param clusterName
   * @param namespaceName
   * @param branchName
   */
  private void checkBranch(String appId, String clusterName, String namespaceName, String branchName) {
    //校验namespace是否存在
    //1. check parent namespace
    checkNamespace(appId, clusterName, namespaceName);

    //校验子namespace是否存在 若不存在，抛出BadRequestException
    //2. check child namespace
    Namespace childNamespace = namespaceService.findOne(appId, branchName, namespaceName);
    if (childNamespace == null) {
      throw new BadRequestException(String.format("Namespace's branch not exist. AppId = %s, ClusterName = %s, "
                                                  + "NamespaceName = %s, BranchName = %s",
                                                  appId, clusterName, namespaceName, branchName));
    }

  }

  private void checkNamespace(String appId, String clusterName, String namespaceName) {
    //查询父namespace对象
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    //若父namespace不存在，则抛出BadRquestException异常
    if (parentNamespace == null) {
      throw new BadRequestException(String.format("Namespace not exist. AppId = %s, ClusterName = %s, NamespaceName = %s", appId,
                                                  clusterName, namespaceName));
    }
  }


}
