package com.commerce.commons.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/7 20:15
 */
@Slf4j
public class CreateMySqlPool {

//    public static QueryRunner getQueryRunner(){
//        return new QueryRunner(getDataSource());
//    }


    public static DataSource getDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://172.16.24.51:3306");
        dataSource.setUsername("sddt");
        dataSource.setPassword("sddt8888");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);
        log.info("创建dataSource：{}", dataSource);
        return dataSource;
    }


    public static Connection getConnection() {
        Connection con = null;
        try {
            con = getDataSource().getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
