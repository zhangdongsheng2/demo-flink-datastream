package com.commerce.commons.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/12 12:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsDosage {
    private String id;

    private String feed_id;

    private Double data_value;

    private Long start_time;

    private Long end_time;

    private String data_time;

    private String step;

    private Date create_time;

    private Date modify_time;
}
