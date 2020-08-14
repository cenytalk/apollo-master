package com.ctrip.framework.apollo.metaservice.controller;

import com.ctrip.framework.apollo.core.ServiceNameConsts;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.metaservice.service.DiscoveryService;
import java.util.Collections;
import java.util.List;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * metaServer的角色主要是为了封装服务发现的细节，对于portal和client而言，永远通过一个http接口
 * 获取admin service和config service的服务信息，而不需要关心背后的服务注册和发现组件
 */
@RestController
@RequestMapping("/services")
public class ServiceController {

  private final DiscoveryService discoveryService;

  public ServiceController(final DiscoveryService discoveryService) {
    this.discoveryService = discoveryService;
  }

  /**
   * This method always return an empty list as meta service is not used at all
   */
  @Deprecated
  @RequestMapping("/meta")
  public List<ServiceDTO> getMetaService() {
    return Collections.emptyList();
  }

  /**
   * 获取 Config Service的服务信息
   * @param appId
   * @param clientIp
   * @return
   */
  @RequestMapping("/config")
  public List<ServiceDTO> getConfigService(
      @RequestParam(value = "appId", defaultValue = "") String appId,
      @RequestParam(value = "ip", required = false) String clientIp) {
    //apollo-configservice
    return discoveryService.getServiceInstances(ServiceNameConsts.APOLLO_CONFIGSERVICE);
  }

  /**
   * 获取Admin Service的服务信息
   * @return
   */
  @RequestMapping("/admin")
  public List<ServiceDTO> getAdminService() {
    //apollo-adminservice
    return discoveryService.getServiceInstances(ServiceNameConsts.APOLLO_ADMINSERVICE);
  }
}
