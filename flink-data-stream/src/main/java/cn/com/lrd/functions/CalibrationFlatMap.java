package cn.com.lrd.functions;

import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.enumeration.FeedValueType;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.model.InputIdValueVo;
import com.commerce.commons.utils.Calculator;
import com.commerce.commons.utils.JedisClusterUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: 校准数据 公式计算
 * @author: zhangdongsheng
 * @date: 2020/5/11 14:39
 */
@Slf4j
public class CalibrationFlatMap extends RichFlatMapFunction<Tuple2<String, InputDataSingle>, Tuple2<String, InputDataSingle>> {
    private transient JedisCluster jedisCluster;
    private String fendTopic;

    public CalibrationFlatMap(String topic) {
        fendTopic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedisCluster = JedisClusterUtil.getJedisCluster(ParameterToolUtil.getParameterTool());
    }

    @Override
    public void flatMap(Tuple2<String, InputDataSingle> value, Collector<Tuple2<String, InputDataSingle>> out) throws Exception {
//        System.out.println(value.f1.getTime() + "=========================");
        Boolean hexists = false;
        try {
            hexists = jedisCluster.hexists(ParameterToolUtil.getParameterTool().get(PropertiesConstants.LARUNDA_INPUT_FEED_KEY), value.f0);
        } catch (Exception e) {
            jedisCluster = JedisClusterUtil.getJedisCluster(ParameterToolUtil.getParameterTool());
            log.info("jedisCluster报错重新获取<<<{}", e.getMessage());
        }
        if (!hexists || StringUtils.isEmpty(value.f1.getCode())) return;

        InputDataSingle inputDataSingle = value.f1;

        String valueIds = jedisCluster.hget(ParameterToolUtil.getParameterTool().get(PropertiesConstants.LARUNDA_INPUT_FEED_KEY), value.f0);
        if (StringUtils.isEmpty(valueIds)) {
            log.debug("数据没有inputId_feedId<<<{}", value.f1);
            return;
        }

        try {
            valueIds = new ObjectMapper().readValue(valueIds, String.class);
        } catch (JsonProcessingException e) {
            log.debug("数据不是 json <<<{}", valueIds);
        }


        List<String> valueList = Arrays.asList(valueIds.split(","));
        inputDataSingle.setInputId(valueList.get(0));
        inputDataSingle.setFeedId(valueList.get(1));

        //发一份原始值
        out.collect(new Tuple2<>(FeedValueType.ORI.getCode(), inputDataSingle));

        String prop = inputDataSingle.getCode();

        //从redis中取出valFeedId和steps
        String feedAndSteps = jedisCluster.hget(fendTopic, inputDataSingle.getInputId());
        Boolean hasFactItem = false;

        String express = null;
        List<InputIdValueVo> computeInputList;
        Map<String, String> computeInputMap = new ConcurrentHashMap<>();
        if (StringUtils.isNotEmpty(feedAndSteps)) {
            hasFactItem = true;
            //切割公式字符串
            List<String> feedAndStepsList = Arrays.asList(feedAndSteps.split("-"));
            //更改数据类型为计算数据
            inputDataSingle.setFeedId(feedAndStepsList.get(0));
            //获取公式
            express = feedAndStepsList.get(1);
            //转换公式的值
            ObjectMapper mapper = new ObjectMapper();
            computeInputList = mapper.readValue(feedAndStepsList.get(2), new TypeReference<List<InputIdValueVo>>() {
            });
            //公式的值存入Map集合
            computeInputList.forEach(inputIdValueVo -> {
                computeInputMap.put(inputIdValueVo.getInputId(), inputIdValueVo.getValue());
            });
            log.debug("计算公式步骤inputId={}, feedId={}, computeInputMap={}, steps={}", inputDataSingle.getInputId(), inputDataSingle.getFeedId(), computeInputMap, express);
        }


        //redis里有公式 需要计算
        if (StringUtils.isNotEmpty(express)) {
            //包含input说明有+-*/input操作、先作预处理
            if (express.contains("input")) {
                String preArry[] = express.split(",");
                for (int i = 0; i < preArry.length; i++) {
                    String pre = preArry[i];
                    if (pre.contains("input")) {
                        String opArray[] = pre.split("input");
                        String inputId = opArray[1];
                        //替换操作符
                        String repChar = "input" + inputId;
                        //取到computeInputId的当前值（和Input的时间点一致）
                        //Double cValue = mapState.get(inputId);
                        express = express.replace(repChar, computeInputMap.get(inputId));
                    }
                }
            }
            //获取计算表达式
            double result = 0d;
            String[] arry = express.split(",");
            for (int i = 0; i < arry.length; i++) {
                if (i == 0) {
                    result = Calculator.conversion(inputDataSingle.getValue() + arry[i]);
                    log.debug("[step" + i + "]计算表达式[" + inputDataSingle.getValue() + arry[i] + "] = " + result);
                } else {
                    String next = result + arry[i];
                    result = Calculator.conversion(next);
                    log.debug("[step" + i + "]计算表达式[" + next + "] = " + result);
                }
            }
            log.debug("计算结果={}", result);
            //如果计算结果有问题则不予保存
            if (!"NaN".equals(String.valueOf(result))) {
                inputDataSingle.setValue(result);
            }
        }
        //有公式的发一份校准值到流中
        if (hasFactItem) {
            out.collect(new Tuple2<>(FeedValueType.VAL.getCode(), inputDataSingle));
        }
    }
}