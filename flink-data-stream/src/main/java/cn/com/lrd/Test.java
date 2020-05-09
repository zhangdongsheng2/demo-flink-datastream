package cn.com.lrd;

import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 13:58
 */
public class Test {
    public static void main(String[] args) throws SQLException {
//        String iotDeviceSql = " SELECT id,dev_seri_no FROM iot_FgeuOtIuaa.iot_device WHERE dev_seri_no=? AND is_del=?";


//        QueryRunner queryRunner = new QueryRunner(CreateMySqlPool.getDataSource());
//        Map<String, Object> query = queryRunner.query(iotDeviceSql, new MapHandler(), "10991099", 0);
//
//        System.out.println(query);
//        Map<String, Object> query1 = queryRunner.query("SELECT * FROM iot_FgeuOtIuaa.topic WHERE device_id=\"4b5124dc-e00d-4c39-bb33-a309a04baf1d\" AND value=\"/pub/a1u4QFBigSjla1qgIeXxqTr0/device99/topic002\" AND is_del=0", new MapHandler());
//
//        System.out.println(query1);

        System.out.println(LocalDateTime.now().toString());
    }
}
