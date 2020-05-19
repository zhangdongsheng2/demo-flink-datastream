package cn.com.lrd.functions.dataset;

import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.config.InfluxDBConfig;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.InfluxDBConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @description: 数据之间输出到时序数据库
 * @author: zhangdongsheng
 * @date: 2020/5/11 11:38
 */
@Slf4j
public class InfluxDBOutput extends RichOutputFormat<InputDataSingle> {

    private transient InfluxDBConfig influxDBConfig;
    private transient InfluxDB influxDBClient;

    @Override
    public void configure(Configuration parameters) {
        influxDBConfig = Preconditions.checkNotNull(InfluxDBConfigUtil.getInfluxDBConfig(ParameterToolUtil.getParameterTool()), "InfluxDB client config should not be null");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
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
    public void writeRecord(InputDataSingle input) throws IOException {
        long time = DateUtil.parseStrDateTime(input.getTime());
        Point.Builder builder = Point.measurement(influxDBConfig.getMeasurement())
                .tag("feedid", input.getFeedId())
                .addField("value", input.getValue())
                .time(time, TimeUnit.MILLISECONDS);

        Point point = builder.build();
        influxDBClient.write(point);
//        log.info("输出数据到influxdb<<< {}",input);
    }

    @Override
    public void close() {
        if (influxDBClient.isBatchEnabled()) {
            influxDBClient.disableBatch();
        }
        influxDBClient.close();
    }
}

