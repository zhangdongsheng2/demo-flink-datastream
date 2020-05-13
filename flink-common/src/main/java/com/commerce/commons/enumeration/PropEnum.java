package com.commerce.commons.enumeration;

/**
 * 需要计算用量的属性.
 *
 * @author yx
 * @version 1.0
 * @since 20-1-10上午10:19
 */
public enum PropEnum {

    /**
     * 正序
     */
    PRO_A29("A29", "正向有功电度(电量)"),
    PRO_B2("B2", "累计水流量"),
    PRO_C2("C2", "累计气流量"),
    PRO_D4("D4", "累计气流量"),
    PRO_E3("E3", "累计汽流量"),
    PRO_G2("G2", "累计气流量"),
    PRO_I2("I2", "累计气流量"),
    PRO_J2("J2", "累计气流量"),
    PRO_F4("F4", "累计氮气流量");

    /**
     * code
     */
    private String code;

    /**
     * name
     */
    private String name;

    PropEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return this.code;
    }

    public String getName() {
        return this.name;
    }

}
