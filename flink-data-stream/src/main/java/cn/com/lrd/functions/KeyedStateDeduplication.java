package cn.com.lrd.functions;

import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.ExecutionEnvUtil;
import com.commerce.commons.utils.JedisClusterUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

/**
 * @description: 生成仪表根据 key 去重
 * key =  inputData.getSn() + "_"
 * + inputData.getCode() + "_"
 * + inputData.getType() + "_"
 * + inputData.getAdd();
 * @author: zhangdongsheng
 * @date: 2020/5/9 08:55
 */
public class KeyedStateDeduplication extends KeyedProcessFunction<Tuple, Tuple2<String, InputDataSingle>, InputDataSingle> {

    // 使用该 ValueState 来标识当前 Key 是否之前存在过
    private ValueState<Boolean> isExist;
    private transient JedisCluster cluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        cluster = JedisClusterUtil.getJedisCluster();

        ValueStateDescriptor<Boolean> keyedStateDuplicated =
                new ValueStateDescriptor<>("KeyedStateDeduplication", TypeInformation.of(new TypeHint<Boolean>() {
                }));
        // 从状态后端恢复状态
        isExist = getRuntimeContext().getState(keyedStateDuplicated);
    }

    @Override
    public void processElement(Tuple2<String, InputDataSingle> stringInputDataTuple2, Context context, Collector<InputDataSingle> collector) throws Exception {
        if (null == isExist.value()) {
            isExist.update(true);
            Boolean hexists = cluster.hexists(ExecutionEnvUtil.getParameterTool().get(PropertiesConstants.LARUNDA_INPUT_FEED_KEY), stringInputDataTuple2.f0);
            if (!hexists)
                collector.collect(stringInputDataTuple2.f1);
        }
    }

}
