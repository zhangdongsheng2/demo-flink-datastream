package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 11:21
 */
public enum DeviceStatus {
    ONLINE("在线"),
    OFFLINE("离线"),
    UNKNOWN("未知"),
    INACTIVE("未激活"),
    ACTIVE("激活");

    private String name;

    private DeviceStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
