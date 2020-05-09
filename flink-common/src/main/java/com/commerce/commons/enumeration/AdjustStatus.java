package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 11:23
 */
public enum AdjustStatus {
    ADJUSTED("已调试"),
    TO_ADJUST("待调试");

    private String name;

    private AdjustStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
