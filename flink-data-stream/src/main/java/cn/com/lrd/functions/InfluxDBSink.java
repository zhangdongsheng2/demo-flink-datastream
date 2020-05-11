package cn.com.lrd.functions;

import com.commerce.commons.config.InfluxDBConfig;
import com.commerce.commons.model.InputDataSingle;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/11 11:38
 */
public class InfluxDBSink extends RichSinkFunction<InputDataSingle> {

    private final InfluxDBConfig influxDBConfig;
    private transient InfluxDB influxDBClient;

    public InfluxDBSink(InfluxDBConfig influxDBConfig) {
        this.influxDBConfig = Preconditions.checkNotNull(influxDBConfig, "InfluxDB client config should not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
        if (influxDBConfig.isCreateDatabase())
            influxDBClient.query(new Query("CREATE DATABASE " + influxDBConfig.getDatabase()));
        influxDBClient.setDatabase(influxDBConfig.getDatabase());

        if (influxDBConfig.getBatchActions() > 0) {
            influxDBClient.enableBatch(influxDBConfig.getBatchActions(), influxDBConfig.getFlushDuration(), influxDBConfig.getFlushDurationTimeUnit());
        }

        if (influxDBConfig.isEnableGzip()) {
            influxDBClient.enableGzip();
        }
    }

    @Override
    public void invoke(InputDataSingle input, SinkFunction.Context context) throws Exception {
        long time = Timestamp.valueOf(LocalDateTime.parse(input.getTime(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).getTime();
        Point.Builder builder = Point.measurement(influxDBConfig.getMeasurement())
                .tag("feedid", input.getFeedId())
                .addField("value", input.getValue())
                .time(time, TimeUnit.MILLISECONDS);

        Point point = builder.build();
        influxDBClient.write(point);
    }

    @Override
    public void close() {
        if (influxDBClient.isBatchEnabled()) {
            influxDBClient.disableBatch();
        }
        influxDBClient.close();
    }
}

