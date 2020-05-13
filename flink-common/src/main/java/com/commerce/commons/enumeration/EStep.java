package com.commerce.commons.enumeration;

/**
 * @author
 **/
public enum EStep {
    /**
     * feed数据保存时间单位
     */
    /**
     * 1分钟
     **/
    ONE_MINUTE("1m", "1分钟"),
    /**
     * 5分钟
     **/
    FIVE_MINUTE("5m", "5分钟"),
    /**
     * 10分钟
     **/
    TEN_MINUTE("10m", "10分钟"),
    /**
     * 15分钟
     **/
    FIFTEEN_MINUTE("15m", "15分钟"),
    /**
     * 30分钟
     **/
    THIRTY_MINUTE("30m", "30分钟"),
    /**
     * 1小时
     **/
    ONE_HOUR("1h", "1小时"),
    /**
     * 1天
     **/
    ONE_DAY("1d", "1天"),
    /**
     * 1月
     **/
    ONE_MONTH("1n", "1月"),
    /**
     * 1年
     **/
    ONE_YEAR("1y", "1年");


    /**
     * name
     */
    private String name;

    /**
     * desc
     */
    private String desc;

    EStep(String name, String desc) {
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
