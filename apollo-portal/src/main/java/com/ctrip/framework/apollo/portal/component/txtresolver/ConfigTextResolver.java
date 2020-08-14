package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;

import java.util.List;

/**
 * users can modify config in text mode.so need resolve text.
 * 配置文本解析器接口
 */
public interface ConfigTextResolver {

  /**
   *  解析文本，创建ItemChangeSets对象
   * @param namespaceId
   * @param configText 配置文本
   * @param baseItems 已存在ItemDTO
   * @return
   */
  ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems);

}
