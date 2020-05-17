package cn.com.lrd.offline;

import cn.com.lrd.functions.CalibrationFlatMap;
import cn.com.lrd.functions.PreprocessorFilterFlatMap;
import cn.com.lrd.functions.PreprocessorOutFlatMap;
import cn.com.lrd.functions.PreprocessorTimeFlatMap;
import cn.com.lrd.utils.ParameterToolUtil;
import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.config.InfluxDBConfig;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.model.*;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.InfluxDBConfigUtil;
import com.commerce.commons.utils.MyEsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
//        String sn = parameterTool.get("sn");
//        String type = parameterTool.get("type");
//        String addr = parameterTool.get("addr");
//        String stime = parameterTool.get("stime");
//        String etime = parameterTool.get("etime");

//        String parameters = parameterTool.get("parameters");
//        List<AlertVo> alertVos = JSONObject.parseArray(parameters, AlertVo.class);
//        for (AlertVo alertVo : alertVos) {
//            run(alertVo, parameterTool);
//        }10001584331440

        AlertVo alertVo = new AlertVo();
        alertVo.setAddr("1");
        alertVo.setStime("2020-05-01 13:12:00");
//        alertVo.setSn(new StringBuilder("01000000").reverse().toString());
        alertVo.setSn("51101111");
        alertVo.setType("1");
        alertVo.setEtime("2020-05-11 19:12:00");

        run(alertVo, parameterTool);
    }


    public static void run(AlertVo args, ParameterTool parameterTool) throws Exception {
        String fendTopic = parameterTool.get("changed.topic");

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
                        String startkey = new StringBuilder(args.getSn()).reverse().toString() + "" + args.getType() + "" + args.getAddr() + "" + start;
                        scan.setStartRow(Bytes.toBytes(startkey));
                        //结束范围
                        String endkey = new StringBuilder(args.getSn()).reverse().toString() + "" + args.getType() + "" + args.getAddr() + "" + end;
                        scan.setStopRow(Bytes.toBytes(endkey));

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
                        Instant instant = Instant.ofEpochMilli(Bytes.toLong(result.getValue(INFO, time)) * 1000);
                        String timeStr = DateUtil.formatLocalDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
                        String topic = Bytes.toString(result.getValue(INFO, OfflineDataSet.topic));
                        int delay = Bytes.toInt(result.getValue(INFO, OfflineDataSet.delay));
                        return new Tuple2<>(key, new InputData(args.getSn(), topic, timeStr, args.getType(), args.getAddr(), codeValueVos, 1, null, delay));
                    }

                })
//                        .setParallelism(1)
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
        tuple2Tuple2FlatMapOperator.mapPartition(new RichMapPartitionFunction<Tuple2<String, InputDataSingle>, Tuple2<String, InputDataSingle>>() {
            private transient InfluxDBConfig influxDBConfig;
            private transient InfluxDB influxDBClient;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                influxDBConfig = Preconditions.checkNotNull(InfluxDBConfigUtil.getInfluxDBConfig(ParameterToolUtil.getParameterTool()), "InfluxDB client config should not be null");
                influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());
                if (influxDBConfig.isCreateDatabase())
                    influxDBClient.query(new Query("CREATE DATABASE " + influxDBConfig.getDatabase()));
                influxDBClient.setDatabase(influxDBConfig.getDatabase());

                if (influxDBConfig.isEnableGzip()) {
                    influxDBClient.enableGzip();
                }
            }

            @Override
            public void mapPartition(Iterable<Tuple2<String, InputDataSingle>> values, Collector<Tuple2<String, InputDataSingle>> out) throws Exception {
                HashSet<Point> points = new HashSet<>();


                values.forEach(new Consumer<Tuple2<String, InputDataSingle>>() {
                    @Override
                    public void accept(Tuple2<String, InputDataSingle> value) {
                        InputDataSingle input = value.f1;
                        long time = DateUtil.parseStrDateTime(input.getTime());
                        Point.Builder builder = Point.measurement(influxDBConfig.getMeasurement())
                                .tag("feedid", input.getFeedId())
                                .addField("value", input.getValue())
                                .time(time, TimeUnit.MILLISECONDS);
                        Point point = builder.build();
                        points.add(point);
                        out.collect(value);
                    }
                });

                log.info("开始输出数据到influxdb<<< {}", points.size());
                influxDBClient.writeWithRetry(BatchPoints.builder().points(points).build());
                log.info("输出数据到influxdb<<< {}", points.size());
            }

            @Override
            public void close() {
                if (influxDBClient.isBatchEnabled()) {
                    influxDBClient.disableBatch();
                }
                influxDBClient.close();
            }
        })
                .flatMap(new PreprocessorFilterFlatMap())
                .flatMap(new PreprocessorTimeFlatMap())
                .flatMap(new PreprocessorOutFlatMap())
                .mapPartition(new MapPartitionFunction<Tuple4<String, String, EsDosagePhase, EsDosage>, Object>() {
                    @Override
                    public void mapPartition(Iterable<Tuple4<String, String, EsDosagePhase, EsDosage>> values, Collector<Object> out) throws Exception {
                        HashMap<String, List<Tuple2<String, Object>>> stringListHashMap = new HashMap<>();

                        values.forEach(new Consumer<Tuple4<String, String, EsDosagePhase, EsDosage>>() {
                            @Override
                            public void accept(Tuple4<String, String, EsDosagePhase, EsDosage> tuple4) {
                                List<Tuple2<String, Object>> tuple2s = stringListHashMap.computeIfAbsent(tuple4.f0, k -> new ArrayList<>());

                                if (tuple4.f0.equals(PreprocessorOutFlatMap.phaseOutputTag)) {
                                    tuple2s.add(new Tuple2<>(tuple4.f1, tuple4.f2));
                                } else {
                                    tuple2s.add(new Tuple2<>(tuple4.f1, tuple4.f3));
                                }

                            }
                        });

                        stringListHashMap.forEach(new BiConsumer<String, List<Tuple2<String, Object>>>() {
                            @Override
                            public void accept(String s, List<Tuple2<String, Object>> tuple2s) {
                                try {
                                    MyEsUtil.executeIndexBulk(s, tuple2s);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });


                    }
                }).print();


//                .map((MapFunction<Tuple2<String, InputDataSingle>, InputDataSingle>) value -> value.f1)
//                .output(new InfluxDBOutput())
//                .getDataSet();


        //用量统计
//        FlatMapOperator<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>,
//                Tuple4<String, String, EsDosagePhase, EsDosage>>
//                tuple6Tuple3FlatMapOperator = tuple2Tuple2FlatMapOperator.flatMap(new PreprocessorFilterFlatMap())
//                .flatMap(new PreprocessorTimeFlatMap())
//                .flatMap(new PreprocessorOutFlatMap());


        //统计数据输出到不同的数据库 ES  1是原始数据
//        outputES("dosage_phase_temp", PreprocessorOutFlatMap.phaseOutputTag, 1, tuple6Tuple3FlatMapOperator);
//        outputES("dosage_half_temp", PreprocessorOutFlatMap.halfTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
//        outputES("dosage_hour_temp", PreprocessorOutFlatMap.hourTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
//        outputES("dosage_day_temp", PreprocessorOutFlatMap.dayTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);
//        outputES("dosage_month_temp", PreprocessorOutFlatMap.monthTimeOutputTag, 2, tuple6Tuple3FlatMapOperator);

    }

    private static void outputES(String index, String tag, int tuple, FlatMapOperator<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>,
            Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>,
            Tuple4<String, String, EsDosagePhase, EsDosage>> tuple6Tuple3FlatMapOperator) {
        tuple6Tuple3FlatMapOperator.filter(new FilterFunction<Tuple4<String, String, EsDosagePhase, EsDosage>>() {
            @Override
            public boolean filter(Tuple4<String, String, EsDosagePhase, EsDosage> value) throws Exception {
                if (tuple == 1) {
                    return tag.equals(value.f0) && value.f3 == null;
                } else {
                    return tag.equals(value.f0) && value.f2 == null;
                }
            }
        }).output(new OutputFormat<Tuple4<String, String, EsDosagePhase, EsDosage>>() {
            @Override
            public void configure(Configuration parameters) {

            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {

            }

            @Override
            public void writeRecord(Tuple4<String, String, EsDosagePhase, EsDosage> record) throws IOException {
                try {
                    ArrayList<Tuple2<String, Object>> objects = new ArrayList<>();
                    if (tuple == 1) {

                        objects.add(new Tuple2<>(record.f1, record.f2));
                    } else {
                        objects.add(new Tuple2<>(record.f1, record.f3));
                    }
                    MyEsUtil.executeIndexBulk(index, objects);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws IOException {

            }
        });


//                .mapPartition(new MapPartitionFunction<Tuple4<String, String, EsDosagePhase, EsDosage>, Iterable<Tuple2<String, Object>>>() {
//            @Override
//            public void mapPartition(Iterable<Tuple4<String, String, EsDosagePhase, EsDosage>> values, Collector<Iterable<Tuple2<String, Object>>> out) throws Exception {
//                ArrayList<Tuple2<String, Object>> esDosagePhases = new ArrayList<>();
//                values.forEach(esDosageTuple4 -> {
//                    if (tuple == 1) {
//                        esDosagePhases.add(new Tuple2<>(esDosageTuple4.f1, esDosageTuple4.f2));
//                    } else {
//                        esDosagePhases.add(new Tuple2<>(esDosageTuple4.f1, esDosageTuple4.f3));
//                    }
//                });
//                out.collect(esDosagePhases);
//            }
//        }).output(new AdapterRichOutputFormat<Iterable<Tuple2<String, Object>>>() {
//
//            @Override
//            public void writeRecord(Iterable<Tuple2<String, Object>> record) {
//                try {
//                    MyEsUtil.executeIndexBulk(index, record);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
    }
}
