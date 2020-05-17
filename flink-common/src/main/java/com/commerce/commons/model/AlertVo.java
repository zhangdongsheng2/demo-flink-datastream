package com.commerce.commons.model;

import lombok.Data;

import java.io.Serializable;

/**
 * sn码 表类型 表地址 开始时间 结束时间
 *
 * @author songdongrui
 */
@Data
public class AlertVo implements Serializable {
    private String sn;
    private String type;
    private String addr;
    private String stime;
    private String etime;
}
