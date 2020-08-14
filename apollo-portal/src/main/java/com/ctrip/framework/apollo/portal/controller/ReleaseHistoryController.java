package com.ctrip.framework.apollo.portal.controller;


import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.portal.component.PermissionValidator;
import com.ctrip.framework.apollo.portal.entity.bo.ReleaseHistoryBO;
import com.ctrip.framework.apollo.portal.service.ReleaseHistoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

/**
 * 发布历史控制器
 */
@RestController
public class ReleaseHistoryController {

  private final ReleaseHistoryService releaseHistoryService;
  private final PermissionValidator permissionValidator;

  public ReleaseHistoryController(final ReleaseHistoryService releaseHistoryService, final PermissionValidator permissionValidator) {
    this.releaseHistoryService = releaseHistoryService;
    this.permissionValidator = permissionValidator;
  }

  /**
   * 查询应用配置发布历史
   * @param appId
   * @param env
   * @param clusterName
   * @param namespaceName
   * @param page
   * @param size
   * @return
   */
  @GetMapping("/apps/{appId}/envs/{env}/clusters/{clusterName}/namespaces/{namespaceName}/releases/histories")
  public List<ReleaseHistoryBO> findReleaseHistoriesByNamespace(@PathVariable String appId,
                                                                @PathVariable String env,
                                                                @PathVariable String clusterName,
                                                                @PathVariable String namespaceName,
                                                                @RequestParam(value = "page", defaultValue = "0") int page,
                                                                @RequestParam(value = "size", defaultValue = "10") int size) {

    if (permissionValidator.shouldHideConfigToCurrentUser(appId, env, namespaceName)) {
      return Collections.emptyList();
    }

   return releaseHistoryService.findNamespaceReleaseHistory(appId, Env.valueOf(env), clusterName ,namespaceName, page, size);
  }

}
