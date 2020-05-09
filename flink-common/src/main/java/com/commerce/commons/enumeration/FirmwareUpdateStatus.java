package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 11:24
 */
public enum FirmwareUpdateStatus {
    SUCCEEDED("成功"),
    FAILED("失败"),
    UPDATING("更新中");

    private String name;

    private FirmwareUpdateStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}