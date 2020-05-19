package cn.com.lrd.functions;

import com.commerce.commons.config.InfluxDBConfig;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @description: 数据之间输出到时序数据库
 * @author: zhangdongsheng
 * @date: 2020/5/11 11:38
 */
@Slf4j
public class InfluxDBSink extends RichSinkFunction<Iterable<Tuple2<String, InputDataSingle>>> {

    private final InfluxDBConfig influxDBConfig;

    public InfluxDBSink(InfluxDBConfig influxDBConfig) {
        this.influxDBConfig = Preconditions.checkNotNull(influxDBConfig, "InfluxDB client config should not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        InfluxDB influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
        if (influxDBConfig.isCreateDatabase())
            influxDBClient.query(new Query("CREATE DATABASE " + influxDBConfig.getDatabase()));
        influxDBClient.setDatabase(influxDBConfig.getDatabase());
        influxDBClient.close();
    }

    @Override
    public void invoke(Iterable<Tuple2<String, InputDataSingle>> inputs, SinkFunction.Context context) throws Exception {
        Set<Point> points = new HashSet<>();
        inputs.forEach(new Consumer<Tuple2<String, InputDataSingle>>() {
            @Override
            public void accept(Tuple2<String, InputDataSingle> tuple2) {
                InputDataSingle input = tuple2.f1;
                long time = DateUtil.parseStrDateTime(input.getTime());
                Point.Builder builder = Point.measurement(influxDBConfig.getMeasurement())
                        .tag("feedid", input.getFeedId())
                        .addField("value", input.getValue())
                        .time(time, TimeUnit.MILLISECONDS);

                Point point = builder.build();
                points.add(point);
            }
        });

        try {
            InfluxDB influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
            BatchPoints build = BatchPoints.database(influxDBConfig.getDatabase()).points(points).build();
            influxDBClient.setLogLevel(InfluxDB.LogLevel.BASIC);
            influxDBClient.writeWithRetry(build);
            influxDBClient.close();
            log.info("InfluxDB 写入数据  条数<<<{}", points.size());
        } catch (Exception e) {
            log.info("数据写入失败{}  条数<<<{}", e.getMessage(), points.size());

            try {
                InfluxDB influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
                BatchPoints build = BatchPoints.database(influxDBConfig.getDatabase()).points(points).build();
                influxDBClient.setLogLevel(InfluxDB.LogLevel.BASIC);
                influxDBClient.writeWithRetry(build);
                influxDBClient.close();
                log.info("InfluxDB ===重新===写入数据  条数<<<{}", points.size());
            } catch (Exception ex) {
                log.info("InfluxDB ===重新===写入数据失败{}  条数<<<{}", e.getMessage(), points.size());
            }

        }


    }
}

