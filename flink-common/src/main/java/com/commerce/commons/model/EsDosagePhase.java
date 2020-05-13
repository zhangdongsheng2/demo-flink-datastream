package com.commerce.commons.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/12 14:31
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsDosagePhase {
    private String id;

    private String feed_id;

    private String prop;

    private Double data_value;

    private String data_time;

    private Date create_time;

    private Date modify_time;

}
