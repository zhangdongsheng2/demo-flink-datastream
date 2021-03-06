package com.commerce.commons.model;

import lombok.Data;

import java.io.Serializable;

/**
 * B-原始数据Kafka消息包含的属性名称和值
 *
 * @author zs
 * @version 1.0
 * @since 2019-07-01
 */
@Data
public class CodeValueVo implements Serializable {

    /**
     * 属性名称
     */
    private String code;

    /**
     * 值
     */
    private double value;
}
