package com.commerce.commons.model;

import lombok.Data;

import java.util.List;

/**
 * @Description InputData
 * 多值input
 * @Author zs
 * @Date 19-8-21 下午5:19
 * @Version 1.0
 **/
@Data
public class InputData {
    /**
     * 设备sn
     */
    private String sn;
    /**
     * topic
     */
    private String topic;
    /**
     * mqtt发送时间
     */
    private String time;
    /**
     * 仪表类型
     */
    private String type;
    /**
     * 仪表地址
     */
    private String add;
    /**
     * 多属性值
     */
    private List<CodeValueVo> data;
    /**
     * 数据来源标记
     * 0: 实时数据，　１：离线数据
     */
    private int from = 0;
}