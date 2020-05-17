package com.commerce.commons.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description InputData
 * 多值input
 * @Author zs
 * @Date 19-8-21 下午5:19
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InputDataSingle implements Serializable {
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
     * 属性名称
     */
    private String code;

    /**
     * 值
     */
    private double value;

    /**
     * 企业的Schema 数据库名字
     */
    private String dsSchema;
    /**
     * 数据来源标记
     * 0: 实时数据，　１：离线数据
     * <p>
     * 离线数据使用 时间参数 重新开一个job  进行处理
     */
//    private int from = 0;


    private String inputId;

    private String feedId;
}