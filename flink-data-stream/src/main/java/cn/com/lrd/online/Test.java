package cn.com.lrd.online;

import com.commerce.commons.utils.DateUtil;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 13:58
 */
public class Test {
    public static void main(String[] args) throws Exception {
//        String iotDeviceSql = " SELECT id,dev_seri_no FROM iot_FgeuOtIuaa.iot_device WHERE dev_seri_no=? AND is_del=?";


//        QueryRunner queryRunner = new QueryRunner(CreateMySqlPool.getDataSource());
//        Map<String, Object> query = queryRunner.query(iotDeviceSql, new MapHandler(), "10991099", 0);
//
//        System.out.println(query);
//        Map<String, Object> query1 = queryRunner.query("SELECT * FROM iot_FgeuOtIuaa.topic WHERE device_id=\"4b5124dc-e00d-4c39-bb33-a309a04baf1d\" AND value=\"/pub/a1u4QFBigSjla1qgIeXxqTr0/device99/topic002\" AND is_del=0", new MapHandler());
//
//        System.out.println(query1);

//        System.out.println(LocalDateTime.now().toString());

//        System.out.println( JedisClusterUtil.getJedisCluster().hgetAll("aaaaaa"));

//查询公式是否存在 根据inputID
//        System.out.println(JedisClusterUtil.getJedisCluster().hget("dev2-rest-b-config-to-calibration", "cdb053c53ea8414fa64c8cb43298b373"));

        //10000015_A10_1_2
//        System.out.println( JedisClusterUtil.getJedisCluster().hget("larunda.input.feed.key","10000015_A10_1_2"));

//        System.out.println(JedisClusterUtil.getJedisCluster().hgetAll(ExecutionEnvUtil.getParameterTool().get(PropertiesConstants.LARUNDA_INPUT_FEED_KEY)));

//        System.out.println( new ObjectMapper().readValue(JedisClusterUtil.getJedisCluster().hget(ExecutionEnvUtil.getParameterTool().get(PropertiesConstants.LARUNDA_INPUT_FEED_KEY),"10000025_A103_1_11"), String.class));


//        System.out.println( JedisPool.getShardedJedisPool().getResource().hget("larunda.input.feed.key","10000015_A10_1_2"));

//kafka 生产者
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        Properties props = buildKafkaProducerProps(parameterTool);
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        ProducerRecord<String, String> record = new ProducerRecord<>(parameterTool.get("notice.topic"),  "1","1");
//        producer.send(record);
//        producer.close();


// Create a database...
// https://docs.influxdata.com/influxdb/v1.7/query_language/database_management/
//        InfluxDBConfig influxDBConfig = InfluxDBConfigUtil.getInfluxDBConfig();
//        InfluxDB influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
//
//        influxDBClient.query(new Query("CREATE DATABASE " + influxDBConfig.getDatabase()));
//        influxDBClient.setDatabase(influxDBConfig.getDatabase());

//        if (influxDBConfig.getBatchActions() > 0) {
//            influxDBClient.enableBatch(influxDBConfig.getBatchActions(), influxDBConfig.getFlushDuration(), influxDBConfig.getFlushDurationTimeUnit());
//        }

//        influxDBClient.write(Point.measurement("cpu")
//                .time(1589250808384l, TimeUnit.MILLISECONDS)
//                .tag("location", "santa_monica")
//                .addField("level description", "below 3 feet")
//                .addField("water_level", 8.666d)
//                .build());


        LocalDateTime yyyyMMddHHmmss = DateUtil.parseLocalDateTime("20200512143700", "yyyyMMddHHmmss");


//        System.out.println(sub(15,11));
    }


    public static double sub(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.subtract(b2).doubleValue();
    }
}













