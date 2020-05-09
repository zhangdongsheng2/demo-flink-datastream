package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 16:50
 */
public enum FeedValueType {
    ORI("ORI", "ori", "原始值"),
    VAL("VAL", "val", "校准值"),
    MAX("MAX", "max", "取一定时间段内的计算值的最大值"),
    MIN("MIN", "min", "取一定时间段内的计算值的最小值"),
    AVG("AVG", "avg", "取一定时间段内计算值的总和（累计值）"),
    SUM("SUM", "sum", "取一定时间段内计算值的平均值");

    private String code;
    private String name;
    private String desc;

    private FeedValueType(String code, String name, String desc) {
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
