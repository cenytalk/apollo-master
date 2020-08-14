package com.ctrip.framework.apollo.portal.spi;

import com.ctrip.framework.apollo.portal.entity.bo.Email;

/**
 * portal接入邮件服务
 * 现支持发送邮件的动作有：普通发布、灰度发布、全量发布、回滚，
 * 通知对象包括：具有namespace编辑和发布权限的人员以及App负责人
 */
public interface EmailService {

  void send(Email email);

}
