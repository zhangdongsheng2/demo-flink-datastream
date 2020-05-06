package cn.com.lrd;

import com.commerce.commons.model.CodeValueVo;
import com.commerce.commons.model.InputData;
import com.commerce.commons.model.InputStreamData;
import com.commerce.commons.schemas.InputDataSchema;
import com.commerce.commons.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.commerce.commons.utils.KafkaConfigUtil.buildKafkaProps;


/**
 * Desc: Flink 消费 kafka 多个 topic、Pattern 类型的 topic
 * Created by zhisheng on 2019-09-22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class InputDataStream {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);
        //kafka topic list
        List<String> topics = Arrays.asList(parameterTool.get("input.topic"), parameterTool.get("logs.topic"));
        System.out.println(props);
        FlinkKafkaConsumer<InputData> consumer = new FlinkKafkaConsumer<InputData>(topics, new InputDataSchema(), props);
        DataStreamSource<InputData> data = env.addSource(consumer);

        data.flatMap((FlatMapFunction<InputData, Object>) (value, out) -> {
            List<CodeValueVo> data1 = value.getData();
            for (CodeValueVo codeValueVo : data1) {
                out.collect(new InputStreamData());
            }
        });


        data.print();
        System.out.println("bbbbbbbbbbbb");
        env.execute("flink kafka connector test");
    }
}
