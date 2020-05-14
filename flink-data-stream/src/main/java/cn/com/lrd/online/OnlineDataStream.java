package cn.com.lrd.online;

import cn.com.lrd.functions.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.model.*;
import com.commerce.commons.schemas.InputDataSchema;
import com.commerce.commons.utils.ESSinkUtil;
import com.commerce.commons.utils.ExecutionEnvUtil;
import com.commerce.commons.utils.InfluxDBConfigUtil;
import com.commerce.commons.utils.JedisClusterUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static com.commerce.commons.utils.KafkaConfigUtil.buildKafkaProducerProps;
import static com.commerce.commons.utils.KafkaConfigUtil.buildKafkaProps;


/**
 * 实时处理
 */
@Slf4j
public class OnlineDataStream {


    //--input.topic topic-pub555555 传参示例
    public static void main(String[] args) throws Exception {
        //启动前准备
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        String fendTopic = parameterTool.get("changed.topic");
        Producer<String, String> producer = new KafkaProducer<>(buildKafkaProducerProps(parameterTool));    //发送通知 获取公式信息
        ProducerRecord<String, String> record = new ProducerRecord<>(parameterTool.get("notice.topic"), "1", "1");
        producer.send(record);
        producer.close();
        JedisClusterUtil.getJedisCluster().del(fendTopic);

        //创建Flink 运行环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);
        //kafka topic list
        List<String> topics = Arrays.asList(parameterTool.get("emq.topic"), parameterTool.get("restream.topic"), parameterTool.get("restream.init.topic"));
        FlinkKafkaConsumer<InputData> consumer = new FlinkKafkaConsumer<>(topics, new InputDataSchema(), props);
        //读取kafka数据
        DataStreamSource<InputData> data = env.addSource(consumer);
        //过滤异常数据, 然后把数据展开.
        SingleOutputStreamOperator<InputData> inputData = data.filter(new FilterFunction<InputData>() {
            @Override
            public boolean filter(InputData value) throws Exception {
                boolean b = StringUtils.isNotEmpty(value.getSn()) && StringUtils.isNotEmpty(value.getAdd());
                if (!b) {
                    //异常数据. 如果记录在这操作
                    log.info("异常数据: {}" + value.getInputData());
                }
                boolean dataNotEmpty = CollectionUtils.isNotEmpty(value.getData());
                if (!dataNotEmpty) {
                    log.info("数值为空数据: {}", value);
                }
                return b && dataNotEmpty;
            }
        });

        SingleOutputStreamOperator<InputDataSingle> singleData = inputData.flatMap(new FlatMapFunction<InputData, InputDataSingle>() {
            @Override
            public void flatMap(InputData value, Collector<InputDataSingle> out) throws Exception {
                for (CodeValueVo datum : value.getData()) {
                    out.collect(new InputDataSingle(value.getSn(), value.getTopic(), value.getTime(), value.getType(), value.getAdd(), datum.getCode(), datum.getValue(), null, null, null));
                }
            }
        });

//        SingleOutputStreamOperator<String> process = singleData.flatMap(new FlatMapFunction<InputDataSingle, InputDataSingle>() {
//            @Override
//            public void flatMap(InputDataSingle value, Collector<InputDataSingle> out) throws Exception {
//                out.collect(value);
//            }
//        }).keyBy(InputDataSingle::getSn)
//                .flatMap(new RichFlatMapFunction<InputDataSingle, Tuple2<String, String>>() {
//                    private ValueState<Boolean> isExist;
//                    private MapState<String, Double> timeState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        MapStateDescriptor<String, Double> timeStateDescriptor = new MapStateDescriptor<>("timeState",
//                                TypeInformation.of(new TypeHint<String>() {
//                                }),
//                                TypeInformation.of(new TypeHint<Double>() {
//                                }));
//                        // 状态 TTL 相关配置，过期时间设定为 36 小时
//                        StateTtlConfig ttlConfig = StateTtlConfig
//                                .newBuilder(Time.hours(36))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .cleanupIncrementally(10, false)
//                                .build();
//                        // 开启 TTL
//                        timeStateDescriptor.enableTimeToLive(ttlConfig);
//                        // 从状态中恢复 timeState
//                        this.timeState = getRuntimeContext().getMapState(timeStateDescriptor);
//                        ValueStateDescriptor<Boolean> keyedStateDuplicated =
//                                new ValueStateDescriptor<>("ValueState", TypeInformation.of(new TypeHint<Boolean>() {
//                                }));
//                        // 从状态后端恢复状态
//                        isExist = getRuntimeContext().getState(keyedStateDuplicated);
//                    }
//
//                    @Override
//                    public void flatMap(InputDataSingle value, Collector<Tuple2<String, String>> out) throws Exception {
//                        System.out.println(this+"==========="+value.getSn());
//                        timeState.put("aaa", 0.0);
//                        if (null == isExist.value()) {
//                            isExist.update(true);
//                            out.collect(new Tuple2<>("InputDataSingleInputDataSingle", value.getSn()));
//                        }
//                    }
//                })
//                .process(new ProcessFunction<Tuple2<String, String>, String>() {
//                    @Override
//                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
//                        ctx.output(new OutputTag<String>(value.f0) {
//                        }, value.f1.toString());
//                    }
//                });
//
//        process.print();
//        process.getSideOutput(new OutputTag<String>("InputDataSingleInputDataSingle") {
//        }).print("aaaaaaaaaaaaaaaaaaaaaa");


        SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2Stream = singleData.map(new MapFunction<InputDataSingle, Tuple2<String, InputDataSingle>>() {
            @Override
            public Tuple2<String, InputDataSingle> map(InputDataSingle inputData) throws Exception {
                String feedInputMapKey = inputData.getSn() + "_"
                        + inputData.getCode() + "_"
                        + inputData.getType() + "_"
                        + inputData.getAdd();
                return new Tuple2<>(feedInputMapKey, inputData);
            }
        });

//        tuple2Stream.print();
        //初始化公式
//        initFeedItem(fendTopic, env, props, JedisClusterUtil.getJedisNodes());

        //创建仪表
//        createInputIdFeedId(tuple2Stream);

        //校准数据
//        SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2SingleOutputStreamOperator = stCalibration(fendTopic, tuple2Stream);

        //校准数据存储
//        stCalibrationSink(tuple2SingleOutputStreamOperator);

        //用量统计
//        stPreprocessor(tuple2SingleOutputStreamOperator);

        env.execute("flink kafka connector");
    }

    /*
   根据实时数据进行创建仪表
    */
    private static void createInputIdFeedId(SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> inputS) {
        inputS.keyBy(0)
                .process(new KeyedStateDeduplication())
                //创建一个连接池, 查询出Schema
                .flatMap(new QuerySchemasFlatMap()).setParallelism(1)
                .addSink(new FeedRichSink()).setParallelism(1);
    }

    /**
     * 初始化公式, 接收公式后存储到 redis提供使用
     */
    private static void initFeedItem(String fendTopic, StreamExecutionEnvironment env, Properties props, Set<InetSocketAddress> jedisClusterNodes) {
        //读取kafka数据
        env.addSource(new FlinkKafkaConsumer<>(fendTopic, new SimpleStringSchema(), props))
                .flatMap(new FlatMapFunction<String, Map<String, Object>>() {
                    @Override
                    public void flatMap(String value, Collector<Map<String, Object>> out) throws Exception {
                        try {
                            log.info("接收到初始化公式>>>>>>>>>steps={}", value);
                            ObjectMapper mapper = new ObjectMapper();
                            out.collect(mapper.readValue(value, Map.class));
                        } catch (IOException e) {
                            log.info("公式解析出错>>>{}", value);
                        }
                    }
                }).addSink(new RedisSink<Map<String, Object>>(new FlinkJedisClusterConfig.Builder().setNodes(jedisClusterNodes).build(), new RedisMapper<Map<String, Object>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 这里必须是 set 操作，通过 MapState 来维护用户集合，
                return new RedisCommandDescription(RedisCommand.HSET, fendTopic);
            }

            @Override
            public String getKeyFromData(Map<String, Object> in) {
                return String.valueOf(in.get("inputId"));
            }

            @Override
            public String getValueFromData(Map<String, Object> map) {
                String feedId = String.valueOf(map.get("feedId"));
                String steps = String.valueOf(map.get("steps"));
                return feedId + "-" + steps + "-" + JSON.toJSONString(map.get("computeInputList"));
            }
        }));
    }


    /**
     * 数据根据公式校准
     */
    private static SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> stCalibration(String fendTopic, SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> data) {
        return data.flatMap(new CalibrationFlatMap(fendTopic));
    }

    /**
     * 校准数据 输出到时序数据库
     */
    private static void stCalibrationSink(SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2SingleOutputStreamOperator) {
        tuple2SingleOutputStreamOperator.map((MapFunction<Tuple2<String, InputDataSingle>, InputDataSingle>) value -> value.f1)
                .addSink(new InfluxDBSink(InfluxDBConfigUtil.getInfluxDBConfig()));
    }


    /**
     * 用量统计
     * 用量统计依赖状态缓存, 如果状态出问题当天和月用量会计算错误.  状态路径不能随便更改
     */
    private static void stPreprocessor(SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2SingleOutputStreamOperator) {
        SingleOutputStreamOperator<EsDosagePhase> process = tuple2SingleOutputStreamOperator
                .flatMap(new PreprocessorFilterFlatMap())
                .flatMap(new PreprocessorTimeFlatMap())
                .keyBy(0)
                .process(new KeyedStatePreprocessor());

        //原始数据输出到原始数据库
        ESSinkUtil.addSink(3, process, new ElasticsearchSinkFunction<EsDosagePhase>() {
            @Override
            public void process(EsDosagePhase esDosagePhase, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(Requests.indexRequest()
                        .index("dosage_phase")
                        .type("_doc")
                        .id(esDosagePhase.getId())
                        .source(JSONObject.toJSONString(esDosagePhase), XContentType.JSON));
            }
        });

        //统计数据输出到不同的数据库 ES
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.halfTimeOutputTag), getESSinkFunc("dosage_half"));
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.hourTimeOutputTag), getESSinkFunc("dosage_day"));
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.dayTimeOutputTag), getESSinkFunc("dosage_hour"));
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.monthTimeOutputTag), getESSinkFunc("dosage_month"));

    }

    private static ElasticsearchSinkFunction<EsDosage> getESSinkFunc(String index) {
        return new ElasticsearchSinkFunction<EsDosage>() {
            @Override
            public void process(EsDosage esDosage, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(Requests.indexRequest()
                        .index(index)
                        .type("_doc")
                        .id(esDosage.getId())
                        .source(JSONObject.toJSONString(esDosage), XContentType.JSON));
            }
        };
    }

}
