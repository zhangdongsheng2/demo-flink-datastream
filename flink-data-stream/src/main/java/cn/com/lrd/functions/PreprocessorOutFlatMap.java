package cn.com.lrd.functions;

import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.enumeration.EStep;
import com.commerce.commons.model.EsDosage;
import com.commerce.commons.model.EsDosagePhase;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.JedisClusterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @description: 数据添加时间阶段
 * @author: zhangdongsheng
 * @date: 2020/5/13 15:30
 */
@Slf4j
public class PreprocessorOutFlatMap extends RichFlatMapFunction<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>, Tuple3<String, EsDosagePhase, EsDosage>> {
    public static final String phaseOutputTag = "phase";
    public static final String halfTimeOutputTag = "halfTime";
    public static final String hourTimeOutputTag = "hourTime";
    public static final String dayTimeOutputTag = "dayTime";
    public static final String monthTimeOutputTag = "monthTime";

    private Map<String, Double> lastState = new HashMap<>();
    private Map<String, Long> lastTime = new HashMap<>();
    private Map<String, Double> timeState = new HashMap<>();
    private transient JedisCluster cluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        cluster = JedisClusterUtil.getJedisCluster();
    }

    @Override
    public void flatMap(Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle> value, Collector<Tuple3<String, EsDosagePhase, EsDosage>> out) throws Exception {
        InputDataSingle inputDataSingle = value.f5;
        long longTime = DateUtil.parseStrDateTime(inputDataSingle.getTime());
        //这里过滤一下乱序数据, 一个feedId 10分钟的基数出现乱序数据就不处理
        Long lastTimeLone = lastTime.get(inputDataSingle.getFeedId());
        if (lastTimeLone != null && longTime < lastTimeLone) {
            log.info("乱序数据不处理>>>{}", inputDataSingle);
            return;
        }
        lastTime.put(inputDataSingle.getFeedId(), longTime);

        EsDosagePhase esDosagePhase = new EsDosagePhase(inputDataSingle.getFeedId() + "_" + inputDataSingle.getTime(), inputDataSingle.getFeedId(), inputDataSingle.getCode(), inputDataSingle.getValue(), inputDataSingle.getTime(), new Date(), new Date());
        out.collect(new Tuple3<>(phaseOutputTag, esDosagePhase, null));

        //当前Feed 上一笔数据
        BigDecimal lastFeedValue = null;
        if (lastState.get(inputDataSingle.getFeedId()) != null)
            lastFeedValue = BigDecimal.valueOf(lastState.get(inputDataSingle.getFeedId()));
        //当前Feed 现在的数据
        BigDecimal currentFeedValue = BigDecimal.valueOf(inputDataSingle.getValue());
        lastState.put(inputDataSingle.getFeedId(), currentFeedValue.doubleValue());
        //现在的数据与上一笔数据的差值
        double subtract = 0.0;
        if (lastFeedValue != null)
            subtract = currentFeedValue.subtract(lastFeedValue).doubleValue();

        Tuple2<LocalDateTime, LocalDateTime> halfTime = value.f1;
        outPutData(timeState, halfTime, out, esDosagePhase, subtract, EStep.THIRTY_MINUTE.getName(), halfTimeOutputTag);

        Tuple2<LocalDateTime, LocalDateTime> hourTime = value.f3;
        outPutData(timeState, hourTime, out, esDosagePhase, subtract, EStep.ONE_HOUR.getName(), hourTimeOutputTag);

        //=================下面用redis 存储阶段开始时间结束时间=============================

        Tuple2<LocalDateTime, LocalDateTime> dayTime = value.f3;
        outPutData(cluster, dayTime, out, esDosagePhase, EStep.ONE_DAY.getName(), dayTimeOutputTag);

        Tuple2<LocalDateTime, LocalDateTime> monthTime = value.f4;
        outPutData(cluster, monthTime, out, esDosagePhase, EStep.ONE_MONTH.getName(), monthTimeOutputTag);
    }

    private void outPutData(JedisCluster cluster, Tuple2<LocalDateTime, LocalDateTime> tupleTime, Collector<Tuple3<String, EsDosagePhase, EsDosage>> out, EsDosagePhase esDosagePhase, String step, String outputTag) throws Exception {
        LocalDateTime localDateTime = DateUtil.parseLocalDateTime(esDosagePhase.getData_time()).withSecond(0);

        String key = esDosagePhase.getFeed_id() + DateUtil.toEpochSecond(tupleTime.f0) + DateUtil.toEpochSecond(tupleTime.f1);
        String startTimeValue = cluster.hget(key, PropertiesConstants.START_TIME);
        double value = 0.0;
        if (StringUtils.isEmpty(startTimeValue)) {
            cluster.hset(key, PropertiesConstants.START_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
        } else {
            String startValue = startTimeValue.split("#")[1];

            boolean equals = localDateTime.equals(tupleTime.f0);
            if (equals) {
                //离线处理, 如果数据时间等于开始时间 就重新设置开始时间数据.
                cluster.hset(key, PropertiesConstants.START_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
                startValue = "0.0";
            }

            BigDecimal subtract = BigDecimal.valueOf(esDosagePhase.getData_value()).subtract(new BigDecimal(startValue));
            value = subtract.doubleValue();
        }

        cluster.hset(key, PropertiesConstants.END_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
        cluster.expire(key, 35 * 24 * 60 * 60);//设置35天过期

        EsDosage esDosage = new EsDosage(esDosagePhase.getId(), esDosagePhase.getFeed_id(), value,
                DateUtil.toEpochSecond(tupleTime.f0), DateUtil.toEpochSecond(tupleTime.f1), DateUtil.formatLocalDateTime(tupleTime.f1)
                , step, new Date(), new Date());

        out.collect(new Tuple3<>(outputTag, null, esDosage));
    }

    private void outPutData(Map<String, Double> curState, Tuple2<LocalDateTime, LocalDateTime> tupleTime, Collector<Tuple3<String, EsDosagePhase, EsDosage>> out, EsDosagePhase esDosagePhase, Double subtract, String step, String outputTag) throws Exception {
        String key = DateUtil.formatTime(tupleTime.f0) + "_" + DateUtil.formatTime(tupleTime.f1);
        if (curState.get(key) == null) {
            curState.put(key, 0.0);
        } else {
            Double aDouble = curState.get(key);
            curState.put(key, aDouble + subtract);
        }
        EsDosage esDosage = new EsDosage(esDosagePhase.getId(), esDosagePhase.getFeed_id(), curState.get(key),
                DateUtil.toEpochSecond(tupleTime.f0), DateUtil.toEpochSecond(tupleTime.f1), DateUtil.formatLocalDateTime(tupleTime.f1)
                , step, new Date(), new Date());

        out.collect(new Tuple3<>(outputTag, null, esDosage));
    }
}