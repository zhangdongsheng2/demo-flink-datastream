package com.commerce.commons.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/11 09:25
 */
@Data
public class InputIdValueVo implements Serializable {
    private String inputId;
    private String value;
}
