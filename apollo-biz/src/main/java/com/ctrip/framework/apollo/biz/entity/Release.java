package com.ctrip.framework.apollo.biz.entity;

import com.ctrip.framework.apollo.common.entity.BaseEntity;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 发布表 release
 * Namespace发布的配置，每个发布包含发布时该Namespace的所有配置
 * 存在于库apolloconfigdb中
 */
@Entity
@Table(name = "Release")
@SQLDelete(sql = "Update Release set isDeleted = 1 where id = ?")
@Where(clause = "isDeleted = 0")
public class Release extends BaseEntity {
  @Column(name = "ReleaseKey", nullable = false)
  private String releaseKey;//发布的Key

  @Column(name = "Name", nullable = false)
  private String name;//发布名字

  @Column(name = "AppId", nullable = false)
  private String appId;

  @Column(name = "ClusterName", nullable = false)
  private String clusterName;

  @Column(name = "NamespaceName", nullable = false)
  private String namespaceName;

  /**
   * 记录每次发布的完整配置 Map
   */
  @Column(name = "Configurations", nullable = false)
  @Lob
  private String configurations;//发布配置

  @Column(name = "Comment", nullable = false)
  private String comment;//发布说明

  @Column(name = "IsAbandoned", columnDefinition = "Bit default '0'")
  private boolean isAbandoned;//是否废弃

  public String getReleaseKey() {
    return releaseKey;
  }

  public String getAppId() {
    return appId;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getComment() {
    return comment;
  }

  public String getConfigurations() {
    return configurations;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getName() {
    return name;
  }

  public void setReleaseKey(String releaseKey) {
    this.releaseKey = releaseKey;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void setConfigurations(String configurations) {
    this.configurations = configurations;
  }

  public void setNamespaceName(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isAbandoned() {
    return isAbandoned;
  }

  public void setAbandoned(boolean abandoned) {
    isAbandoned = abandoned;
  }

  public String toString() {
    return toStringHelper().add("name", name).add("appId", appId).add("clusterName", clusterName)
        .add("namespaceName", namespaceName).add("configurations", configurations)
        .add("comment", comment).add("isAbandoned", isAbandoned).toString();
  }
}
