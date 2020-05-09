package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 15:17
 */
public enum BsnStatusEnum {
    VALID("有效"),
    INVALID("有效");

    private String name;

    private BsnStatusEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
