package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 16:48
 */
public enum FeedType {
    VIRTUAL("VIRTUAL", "virtual", "虚拟"),
    NORMAL("NORMAL", "normal", "普通");

    private String code;
    private String name;
    private String desc;

    private FeedType(String code, String name, String desc) {
        this.code = code;
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public String getDesc() {
        return this.desc;
    }

    public String getCode() {
        return this.code;
    }
}
