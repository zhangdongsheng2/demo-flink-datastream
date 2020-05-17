package com.commerce.commons.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description InputStreamData
 * 发送给流式计算的对象
 * @Author sym
 * @Date 19-11-01
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InputStreamData implements Serializable {
    private String inputId;
    private String feedId;
    private String topicId;
    private String prop;
    private Double value;
    private long time;
    /**
     * 数据来源标记
     * 0: 实时数据，　１：离线数据
     */
    private int from;
}