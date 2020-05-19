package cn.com.lrd.online;

import cn.com.lrd.utils.EnvUtils;
import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.model.CodeValueVo;
import com.commerce.commons.model.InputData;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.schemas.InputDataSchema;
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
public class KafkaTest {


    //--input.topic topic-pub555555 传参示例
    public static void main(String[] args) throws Exception {
        //启动前准备
        final ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        String fendTopic = parameterTool.get("changed.topic");
//        Producer<String, String> producer = new KafkaProducer<>(buildKafkaProducerProps(parameterTool));    //发送通知 获取公式信息
//        ProducerRecord<String, String> record = new ProducerRecord<>(parameterTool.get("notice.topic"), "1", "1");
//        producer.send(record);
//        producer.close();
//        JedisClusterUtil.getJedisCluster(ParameterToolUtil.getParameterTool()).del(fendTopic);

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

        tuple2Stream.print();

        env.execute("online data stream");
    }


}
