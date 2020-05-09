package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 16:51
 */
public enum FeedLogValueType {
    INSTANT("INSTANT", "sum", "瞬时值"),
    RANGE("RANGE", "avg", "时间段内计算值");

    private String code;
    private String name;
    private String desc;

    private FeedLogValueType(String code, String name, String desc) {
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
