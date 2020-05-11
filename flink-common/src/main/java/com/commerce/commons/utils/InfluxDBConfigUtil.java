package com.commerce.commons.utils;

import com.commerce.commons.config.InfluxDBConfig;
import com.commerce.commons.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/11 11:42
 */
public class InfluxDBConfigUtil {

    public static InfluxDBConfig getInfluxDBConfig() {
        ParameterTool parameterTool = ExecutionEnvUtil.getParameterTool();
        //请将下面的这些字段弄成常量
        return InfluxDBConfig.builder()
                .url(parameterTool.get(PropertiesConstants.INFLUXDB_URL))
                .username(parameterTool.get(PropertiesConstants.INFLUXDB_USERNAME))
                .password(parameterTool.get(PropertiesConstants.INFLUXDB_PASSWORD))
                .database(parameterTool.get(PropertiesConstants.INFLUXDB_DATABASE))
                .batchActions(parameterTool.getInt(PropertiesConstants.INFLUXDB_BATCHACTIONS))
                .flushDuration(parameterTool.getInt(PropertiesConstants.INFLUXDB_FLUSHDURATION))
                .flushDurationTimeUnit(TimeUnit.MILLISECONDS)
                .enableGzip(parameterTool.getBoolean(PropertiesConstants.INFLUXDB_ENABLEGZIP))
                .createDatabase(parameterTool.getBoolean(PropertiesConstants.INFLUXDB_CREATEDATABASE))
                .build();
    }
}
