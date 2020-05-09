package cn.com.lrd;

import cn.com.lrd.functions.FeedRichSink;
import cn.com.lrd.functions.KeyedStateDeduplication;
import cn.com.lrd.functions.QuerySchemasFlatMap;
import com.commerce.commons.model.CodeValueVo;
import com.commerce.commons.model.InputData;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.schemas.InputDataSchema;
import com.commerce.commons.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.commerce.commons.utils.KafkaConfigUtil.buildKafkaProps;


/**
 * 实时处理
 */
@Slf4j
public class InputDataStream {

    //--input.topic topic-pub555555 传参示例
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);
        //kafka topic list
        List<String> topics = Arrays.asList(parameterTool.get("input.topic"), parameterTool.get("logs.topic"));
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
                    out.collect(new InputDataSingle(value.getSn(), value.getTopic(), value.getTime(), value.getType(), value.getAdd(), datum.getCode(), datum.getValue(), null));
                }
            }
        });

        createInputIdFeedId(singleData);

//        stCalibration(inputS);

        env.execute("flink kafka connector");
    }

    /*
     根据实时数据进行创建仪表
     */
    private static void createInputIdFeedId(SingleOutputStreamOperator<InputDataSingle> inputS) {
        inputS.map(new MapFunction<InputDataSingle, Tuple2<String, InputDataSingle>>() {
            @Override
            public Tuple2<String, InputDataSingle> map(InputDataSingle inputData) throws Exception {
                String feedInputMapKey = inputData.getSn() + "_"
                        + inputData.getCode() + "_"
                        + inputData.getType() + "_"
                        + inputData.getAdd();
                return new Tuple2<>(feedInputMapKey, inputData);
            }
        })
                .keyBy(0)
                .process(new KeyedStateDeduplication())
                //创建一个连接池, 查询出Schema
                .flatMap(new QuerySchemasFlatMap()).setParallelism(1)
                .addSink(new FeedRichSink()).setParallelism(1)
        ;
    }


    /**
     * 数据根据公式校准
     */
    private static void stCalibration(SingleOutputStreamOperator<InputData> data) {

        SingleOutputStreamOperator<InputData> inputS = data.flatMap(new FlatMapFunction<InputData, InputData>() {
            @Override
            public void flatMap(InputData value, Collector<InputData> out) throws Exception {
                List<CodeValueVo> data1 = value.getData();
                for (CodeValueVo codeValueVo : data1) {
                    out.collect(new InputData());
                }
            }
        });

        inputS.map(new MapFunction<InputData, Object>() {
            @Override
            public Object map(InputData value) throws Exception {
                return "dddddd";
            }
        }).print();
    }
}






