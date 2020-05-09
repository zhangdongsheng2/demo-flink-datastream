package com.commerce.commons.enumeration;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 16:50
 */
public enum IntervalUnitType {
    SECOND("s", "秒"),
    MINUTE("min", "分"),
    HOUR("h", "时"),
    DAY("d", "天"),
    MONTH("month", "月");

    private String name;
    private String desc;

    private IntervalUnitType(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public String getDesc() {
        return this.desc;
    }
}
