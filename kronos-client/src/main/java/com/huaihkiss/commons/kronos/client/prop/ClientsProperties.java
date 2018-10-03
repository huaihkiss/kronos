package com.huaihkiss.commons.kronos.client.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix="kronos.job")
public class ClientsProperties {
    private Boolean enabled;
    private String zookeeperClusterAddress;
    private String scanPackage;

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public String getZookeeperClusterAddress() {
        return zookeeperClusterAddress;
    }

    public void setZookeeperClusterAddress(String zookeeperClusterAddress) {
        this.zookeeperClusterAddress = zookeeperClusterAddress;
    }

    public String getScanPackage() {
        return scanPackage;
    }

    public void setScanPackage(String scanPackage) {
        this.scanPackage = scanPackage;
    }
}
