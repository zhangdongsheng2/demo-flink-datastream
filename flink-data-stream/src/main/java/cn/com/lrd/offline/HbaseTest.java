package cn.com.lrd.offline;

import cn.com.lrd.functions.hbase.CustomTableInputFormat;
import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.utils.DateUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static cn.com.lrd.offline.OfflineDataSet.*;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/29 09:56
 */
public class HbaseTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 1));

        env.createInput(new CustomTableInputFormat<Tuple2<String, String>>() {

            @SneakyThrows
            @Override
            public void configure(Configuration parameters) {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set(PropertiesConstants.HBASE_ZOOKEEPER_QUORUM, ParameterToolUtil.getParameterTool().get(PropertiesConstants.HBASE_ZOOKEEPER_QUORUM));
                configuration.set(PropertiesConstants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, ParameterToolUtil.getParameterTool().get(PropertiesConstants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
                configuration.set(PropertiesConstants.HBASE_RPC_TIMEOUT, ParameterToolUtil.getParameterTool().get(PropertiesConstants.HBASE_RPC_TIMEOUT));
                configuration.set(PropertiesConstants.HBASE_CLIENT_OPERATION_TIMEOUT, ParameterToolUtil.getParameterTool().get(PropertiesConstants.HBASE_CLIENT_OPERATION_TIMEOUT));
                configuration.set(PropertiesConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, ParameterToolUtil.getParameterTool().get(PropertiesConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));

                Connection connect = ConnectionFactory.createConnection(configuration);


                table = (HTable) connect.getTable(TableName.valueOf(HBASE_TABLE_NAME));
                scan = getScanner();
            }

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(INFO, shuju);
                scan.addColumn(INFO, time);
                scan.addColumn(INFO, topic);
                scan.addColumn(INFO, delay);

                // 0为正常
                SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("info"), Bytes.toBytes("status"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(0));
                scan.setFilter(filter);
//                scan.setReversed(true);
                scan.setFilter(new PageFilter(10));
                return scan;
            }

            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
//                List<CodeValueVo> codeValueVos = JSONObject.parseArray(Bytes.toString(result.getValue(INFO, shuju)), CodeValueVo.class);
                Instant instant = Instant.ofEpochMilli(Bytes.toLong(result.getValue(INFO, time)) * 1000);
                String timeStr = DateUtil.formatLocalDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
                String topic = Bytes.toString(result.getValue(INFO, OfflineDataSet.topic));
//                int delay = Bytes.toInt(result.getValue(INFO, OfflineDataSet.delay));

                return new Tuple2<>(Bytes.toString(result.getValue(INFO, shuju)) + "===" + timeStr + "--" + topic + "---" + delay, "1");
            }
        })
                .setParallelism(1)
                .output(new OutputFormat<Tuple2<String, String>>() {
                    @Override
                    public void configure(Configuration parameters) {

                    }

                    @Override
                    public void open(int taskNumber, int numTasks) throws IOException {

                    }

                    @Override
                    public void writeRecord(Tuple2<String, String> record) throws IOException {
                        System.out.println(record);
                    }

                    @Override
                    public void close() throws IOException {

                    }
                });
        env.execute();
    }
}
