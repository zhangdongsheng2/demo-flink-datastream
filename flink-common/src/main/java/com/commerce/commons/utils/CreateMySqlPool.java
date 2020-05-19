package com.commerce.commons.utils;

import com.commerce.commons.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * @description: 数据库连接池
 * @author: zhangdongsheng
 * @date: 2020/5/7 20:15
 */
@Slf4j
public class CreateMySqlPool {

    public static DataSource getDataSource(ParameterTool parameterTool) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(parameterTool.get(PropertiesConstants.JDBC_DRIVER));
        dataSource.setUrl(parameterTool.get(PropertiesConstants.JDBC_URL));
        dataSource.setUsername(parameterTool.get(PropertiesConstants.JDBC_USERNAME));
        dataSource.setPassword(parameterTool.get(PropertiesConstants.JDBC_PASSWORD));
        //设置连接池的一些参数
        dataSource.setInitialSize(6);
        dataSource.setMaxIdle(9);
        dataSource.setMinIdle(3);
        log.info("创建dataSource：{}", dataSource);
        return dataSource;
    }


    public static Connection getConnection(ParameterTool parameterTool) {
        Connection con = null;
        try {
            con = getDataSource(parameterTool).getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
