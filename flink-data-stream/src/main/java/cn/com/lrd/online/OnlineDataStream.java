package cn.com.lrd.online;

import cn.com.lrd.functions.*;
import cn.com.lrd.utils.EnvUtils;
import cn.com.lrd.utils.JedisClusterUtil;
import cn.com.lrd.utils.ParameterToolUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.model.*;
import com.commerce.commons.schemas.InputDataSchema;
import com.commerce.commons.utils.ESSinkUtil;
import com.commerce.commons.utils.InfluxDBConfigUtil;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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
 * 灰度发布方法:
 * 对旧的任务做过滤规则, 指定的sn 不处理.
 * 开新的任务处理指定的sn 然后检查数据后在做是否完全替换.
 * <p>
 * 实时处理
 */
@Slf4j
public class OnlineDataStream {


    //--profiles.active application_saas.properties   传参设置配置文件示例 .
    public static void main(String[] args) throws Exception {
        //启动前准备
        final ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        String fendTopic = parameterTool.get("changed.topic");
        //发送通知 获取公式信息
        Producer<String, String> producer = new KafkaProducer<>(buildKafkaProducerProps(parameterTool));
        ProducerRecord<String, String> record = new ProducerRecord<>(parameterTool.get("notice.topic"), "1", "1");
        producer.send(record);
        producer.close();
        JedisClusterUtil.del(fendTopic);

        //创建Flink 运行环境
        StreamExecutionEnvironment env = EnvUtils.prepare(parameterTool);
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
                    log.debug("异常数据: {}" + value.getInputData());
                }
                boolean dataNotEmpty = CollectionUtils.isNotEmpty(value.getData());
                if (!dataNotEmpty) {
                    log.debug("数值为空数据: {}", value);
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
        initFeedItem(fendTopic, env, props, JedisClusterUtil.getJedisNodes(ParameterToolUtil.getParameterTool()));

        //创建仪表
        createInputIdFeedId(tuple2Stream);

        //校准数据
        SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2SingleOutputStreamOperator = stCalibration(fendTopic, tuple2Stream);

        //校准数据存储
        stCalibrationSink(tuple2SingleOutputStreamOperator);

        //用量统计
        stPreprocessor(tuple2SingleOutputStreamOperator);

        env.execute("online data stream");
    }

    /*
   根据实时数据进行创建仪表
    */
    private static void createInputIdFeedId(SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> inputS) {
        inputS.keyBy(0)
                .process(new KeyedStateDeduplication())
                //创建一个连接池, 查询出Schema
                .flatMap(new QuerySchemasFlatMap()).setParallelism(1)
                .addSink(new FeedRichSink()).setParallelism(1).name("MySQLSink 创建仪表");
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
                            log.debug("接收到初始化公式>>>>>>>>>steps={}", value);
                            ObjectMapper mapper = new ObjectMapper();
                            out.collect(mapper.readValue(value, Map.class));
                        } catch (IOException e) {
                            log.debug("公式解析出错>>>{}", value);
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
        })).name("Redis公式Sink");
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
        tuple2SingleOutputStreamOperator.timeWindowAll(Time.seconds(6)).process(new ProcessAllWindowFunction<Tuple2<String, InputDataSingle>, Iterable<Tuple2<String, InputDataSingle>>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<String, InputDataSingle>> iterable, Collector<Iterable<Tuple2<String, InputDataSingle>>> collector) throws Exception {
                collector.collect(iterable);
            }
        }).addSink(new InfluxDBSink(InfluxDBConfigUtil.getInfluxDBConfig(ParameterToolUtil.getParameterTool()))).name("InfluxDBSink 校准数据");
    }


    /**
     * 用量统计
     * 用量统计依赖状态缓存, 如果状态出问题当天和月用量会计算错误.  状态路径不能随便更改
     */
    private static void stPreprocessor(SingleOutputStreamOperator<Tuple2<String, InputDataSingle>> tuple2SingleOutputStreamOperator) {
        SingleOutputStreamOperator<Tuple2<String, EsDosagePhase>> process = tuple2SingleOutputStreamOperator
                .flatMap(new PreprocessorFilterFlatMap())
                .flatMap(new PreprocessorTimeFlatMap())
                .keyBy(0)
                .process(new KeyedStatePreprocessor());

        //原始数据输出到原始数据库
        ESSinkUtil.addSink(3, process, new ElasticsearchSinkFunction<Tuple2<String, EsDosagePhase>>() {
            @Override
            public void process(Tuple2<String, EsDosagePhase> tuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(Requests.indexRequest()
                        .index(ParameterToolUtil.getParameterTool().get(PropertiesConstants.INDEX_DOSAGE_PHASE))
                        .type("_doc")
                        .id(tuple2.f0)
                        .source(JSONObject.toJSONString(tuple2.f1), XContentType.JSON));
                log.info("ES 写入数据dosage_phase <<<{}", tuple2.f1);
            }
        }, ParameterToolUtil.getParameterTool());

        //统计数据输出到不同的数据库 ES
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.halfTimeOutputTag), getESSinkFunc(ParameterToolUtil.getParameterTool().get(PropertiesConstants.INDEX_DOSAGE_HALF)), ParameterToolUtil.getParameterTool());
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.hourTimeOutputTag), getESSinkFunc(ParameterToolUtil.getParameterTool().get(PropertiesConstants.INDEX_DOSAGE_HOUR)), ParameterToolUtil.getParameterTool());
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.dayTimeOutputTag), getESSinkFunc(ParameterToolUtil.getParameterTool().get(PropertiesConstants.INDEX_DOSAGE_DAY)), ParameterToolUtil.getParameterTool());
        ESSinkUtil.addSink(3, process.getSideOutput(KeyedStatePreprocessor.monthTimeOutputTag), getESSinkFunc(ParameterToolUtil.getParameterTool().get(PropertiesConstants.INDEX_DOSAGE_MONTH)), ParameterToolUtil.getParameterTool());

    }

    private static ElasticsearchSinkFunction<Tuple2<String, EsDosage>> getESSinkFunc(String index) {
        return new ElasticsearchSinkFunction<Tuple2<String, EsDosage>>() {
            @Override
            public void process(Tuple2<String, EsDosage> tuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(Requests.indexRequest()
                        .index(index)
                        .type("_doc")
                        .id(tuple2.f0)
                        .source(JSONObject.toJSONString(tuple2.f1), XContentType.JSON));
                log.info("ES 写入数据" + index + " <<<{}", tuple2);
            }
        };
    }

}
