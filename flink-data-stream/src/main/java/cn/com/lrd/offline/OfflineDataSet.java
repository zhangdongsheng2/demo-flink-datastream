package cn.com.lrd.offline;

import cn.com.lrd.functions.*;
import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.model.*;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.ExecutionEnvUtil;
import com.commerce.commons.utils.MyEsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * @description: 离线数据处理
 * @author: zhangdongsheng
 * @date: 2020/5/13 11:35
 */
@Slf4j
public class OfflineDataSet {


    //表名
    public static final String HBASE_TABLE_NAME = "IOT_DATA";
    // 列族
    static final byte[] INFO = "info".getBytes(ConfigConstants.DEFAULT_CHARSET);
    //列名
    static final byte[] shuju = "shuju".getBytes(ConfigConstants.DEFAULT_CHARSET);
    static final byte[] time = "time".getBytes(ConfigConstants.DEFAULT_CHARSET);
    static final byte[] topic = "topic".getBytes(ConfigConstants.DEFAULT_CHARSET);
    static final byte[] delay = "delay".getBytes(ConfigConstants.DEFAULT_CHARSET);


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        String sn = parameterTool.get("sn");
//        String type = parameterTool.get("type");
//        String addr = parameterTool.get("addr");
//        String stime = parameterTool.get("stime");
//        String etime = parameterTool.get("etime");

        String parameters = parameterTool.get("parameters");
        List<AlertVo> alertVos = JSONObject.parseArray(parameters, AlertVo.class);
        for (AlertVo alertVo : alertVos) {
            run(alertVo, parameterTool);
        }
    }


    public static void run(AlertVo args, ParameterTool parameterTool) throws Exception {

        String fendTopic = parameterTool.get("changed.topic");

//        String sn = "010000001";
//        String type = "1";
//        String addr = "1000";


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 1));
        FlatMapOperator<Tuple2<String, InputDataSingle>, Tuple2<String, InputDataSingle>> tuple2Tuple2FlatMapOperator =
                env.createInput(new TableInputFormat<Tuple2<String, InputData>>() {

                    @Override
                    protected Scan getScanner() {
                        Scan scan = new Scan();
                        long start = DateUtil.parseStrDateTimeSec(args.getStime());
                        long end = DateUtil.parseStrDateTimeSec(args.getEtime());
                        scan.addColumn(INFO, shuju);
                        scan.addColumn(INFO, time);
                        scan.addColumn(INFO, topic);
                        scan.addColumn(INFO, delay);
                        //开始范围
//                        scan.setStartRow(Bytes.toBytes("01000000110001583032500"));
                        //结束范围
//                        scan.setStopRow(Bytes.toBytes("01000000110001588302900"));

                        //开始范围
                        scan.setStartRow(Bytes.toBytes(new StringBuilder(args.getSn()).reverse().toString() + "" + args.getType() + "" + args.getAddr() + "" + start));
                        //结束范围
                        scan.setStopRow(Bytes.toBytes(new StringBuilder(args.getSn()).reverse().toString() + "" + args.getType() + "" + args.getAddr() + "" + end));

                        // 0为正常
                        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("info"), Bytes.toBytes("status"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(0));
                        scan.setFilter(filter);
//                scan.setReversed(true);
//                scan.setFilter(new PageFilter(10));
                        return scan;
                    }

                    @Override
                    protected String getTableName() {
                        return HBASE_TABLE_NAME;
                    }

                    @Override
                    protected Tuple2<String, InputData> mapResultToTuple(Result result) {
                        String key = Bytes.toString(result.getRow());
                        List<CodeValueVo> codeValueVos = JSONObject.parseArray(Bytes.toString(result.getValue(INFO, shuju)), CodeValueVo.class);
                        Instant instant = Instant.ofEpochMilli(Bytes.toLong(result.getValue(INFO, time)));
                        String timeStr = DateUtil.formatLocalDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
                        String topic = Bytes.toString(result.getValue(INFO, OfflineDataSet.topic));
                        int delay = Bytes.toInt(result.getValue(INFO, OfflineDataSet.delay));
                        return new Tuple2<>(key, new InputData(args.getSn(), topic, timeStr, args.getType(), args.getAddr(), codeValueVos, 1, null, delay));
                    }

                })
                        .setParallelism(1)
                        .flatMap(new FlatMapFunction<Tuple2<String, InputData>, Tuple2<String, InputDataSingle>>() {
                            @Override
                            public void flatMap(Tuple2<String, InputData> value, Collector<Tuple2<String, InputDataSingle>> out) throws Exception {
                                InputData inputData = value.f1;
                                for (CodeValueVo datum : inputData.getData()) {
                                    String feedInputMapKey = inputData.getSn() + "_"
                                            + datum.getCode() + "_"
                                            + inputData.getType() + "_"
                                            + inputData.getAdd();

                                    out.collect(new Tuple2<>(feedInputMapKey,
                                            new InputDataSingle(inputData.getSn(), inputData.getTopic(), inputData.getTime(), inputData.getType(), inputData.getAdd(), datum.getCode(), datum.getValue(),
                                                    null, null, null)));
                                }
                            }
                        })
                        //校准数据
                        .flatMap(new CalibrationFlatMap(fendTopic));


        //保存校准数据
        tuple2Tuple2FlatMapOperator
                .map((MapFunction<Tuple2<String, InputDataSingle>, InputDataSingle>) value -> value.f1)
                .output(new InfluxDBOutput());

        //用量统计
        FlatMapOperator<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>,
                Tuple3<String, EsDosagePhase, EsDosage>>
                tuple6Tuple3FlatMapOperator = tuple2Tuple2FlatMapOperator.flatMap(new PreprocessorFilterFlatMap())
                .flatMap(new PreprocessorTimeFlatMap())
                .flatMap(new PreprocessorOutFlatMap());


        //统计数据输出到不同的数据库 ES
        outputES("dosage_phase", PreprocessorOutFlatMap.phaseOutputTag, 1, tuple6Tuple3FlatMapOperator);
        outputES("dosage_half", PreprocessorOutFlatMap.halfTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
        outputES("dosage_hour", PreprocessorOutFlatMap.hourTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
        outputES("dosage_day", PreprocessorOutFlatMap.dayTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
        outputES("dosage_month", PreprocessorOutFlatMap.monthTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
    }

    private static void outputES(String index, String tag, int tuple, FlatMapOperator<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>,
            Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>,
            Tuple3<String, EsDosagePhase, EsDosage>> tuple6Tuple3FlatMapOperator) {
        tuple6Tuple3FlatMapOperator.filter(new FilterFunction<Tuple3<String, EsDosagePhase, EsDosage>>() {
            @Override
            public boolean filter(Tuple3<String, EsDosagePhase, EsDosage> value) throws Exception {
                if (tuple == 1) {
                    return tag.equals(value.f0) && value.f2 == null;
                } else {
                    return tag.equals(value.f0) && value.f2 == null;
                }
            }
        }).mapPartition((MapPartitionFunction<Tuple3<String, EsDosagePhase, EsDosage>, Iterable<Object>>) (values, out) -> {
            ArrayList<Object> esDosagePhases = new ArrayList<>();
            values.forEach(esDosageTuple3 -> {
                if (tuple == 1) {
                    esDosagePhases.add(esDosageTuple3.f1);
                } else {
                    esDosagePhases.add(esDosageTuple3.f2);
                }
            });
            out.collect(esDosagePhases);
        }).output(new AdapterRichOutputFormat<Iterable<Object>>() {

            @Override
            public void writeRecord(Iterable<Object> record) {
                try {
                    MyEsUtil.executeIndexBulk(index, record, "id");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
