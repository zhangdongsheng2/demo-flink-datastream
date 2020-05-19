package cn.com.lrd.functions;

import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.enumeration.EStep;
import com.commerce.commons.model.EsDosage;
import com.commerce.commons.model.EsDosagePhase;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.JedisClusterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * @description: 用量统计, 原始数据 10分钟一条, 统计出, 半小时,一小时,一天,一月的用量.  输出到侧输出流, Sink到不同的数据库
 * @author: zhangdongsheng
 * @date: 2020/5/12 16:37
 */
@Slf4j
public class KeyedStatePreprocessor extends KeyedProcessFunction<Tuple, Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>, Tuple2<String, EsDosagePhase>> {
    public static final OutputTag<Tuple2<String, EsDosage>> halfTimeOutputTag = new OutputTag<Tuple2<String, EsDosage>>("halfTime") {
    };
    public static final OutputTag<Tuple2<String, EsDosage>> hourTimeOutputTag = new OutputTag<Tuple2<String, EsDosage>>("hourTime") {
    };
    public static final OutputTag<Tuple2<String, EsDosage>> dayTimeOutputTag = new OutputTag<Tuple2<String, EsDosage>>("dayTime") {
    };
    public static final OutputTag<Tuple2<String, EsDosage>> monthTimeOutputTag = new OutputTag<Tuple2<String, EsDosage>>("monthTime") {
    };



    private ValueState<Long> lastTime;
    private transient JedisCluster cluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        cluster = JedisClusterUtil.getJedisCluster(ParameterToolUtil.getParameterTool());
        // 状态 TTL 相关配置，过期时间设定为 36 小时
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot() //1.7 版本的.
//                .cleanupIncrementally(10, false)  //1.10.0 版本的方法
                .build();
        ValueStateDescriptor<Long> lastTime = new ValueStateDescriptor<>("lastTime", TypeInformation.of(new TypeHint<Long>() {
        }));
        lastTime.enableTimeToLive(ttlConfig);
        this.lastTime = getRuntimeContext().getState(lastTime);
    }

    @Override
    public void processElement(Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle> value, Context ctx, Collector<Tuple2<String, EsDosagePhase>> out) throws Exception {
        InputDataSingle inputDataSingle = value.f5;
        long longTime = DateUtil.parseStrDateTime(inputDataSingle.getTime());
        //这里过滤一下乱序数据, 一个feedId 10分钟的基数出现乱序数据就不处理
        if (lastTime.value() != null && longTime < lastTime.value()) {
            log.info("乱序数据不处理>>>{}", inputDataSingle);
            return;
        }
        lastTime.update(longTime);

        EsDosagePhase esDosagePhase = new EsDosagePhase(inputDataSingle.getFeedId(), inputDataSingle.getCode(), inputDataSingle.getValue(), inputDataSingle.getTime(), new Date(), new Date());
        out.collect(new Tuple2<>(inputDataSingle.getFeedId() + "_" + inputDataSingle.getTime(), esDosagePhase));


        //时间存入半小时
        //=================下面用redis 存储阶段开始时间结束时间=============================
        Tuple2<LocalDateTime, LocalDateTime> halfTime = value.f1;
        outPutData(cluster, halfTime, ctx, esDosagePhase, EStep.THIRTY_MINUTE.getName(), halfTimeOutputTag);

        Tuple2<LocalDateTime, LocalDateTime> hourTime = value.f2;
        outPutData(cluster, hourTime, ctx, esDosagePhase, EStep.ONE_HOUR.getName(), hourTimeOutputTag);

        Tuple2<LocalDateTime, LocalDateTime> dayTime = value.f3;
        outPutData(cluster, dayTime, ctx, esDosagePhase, EStep.ONE_DAY.getName(), dayTimeOutputTag);

        Tuple2<LocalDateTime, LocalDateTime> monthTime = value.f4;
        outPutData(cluster, monthTime, ctx, esDosagePhase, EStep.ONE_MONTH.getName(), monthTimeOutputTag);
    }

    private void outPutData(JedisCluster cluster, Tuple2<LocalDateTime, LocalDateTime> tupleTime, Context ctx, EsDosagePhase esDosagePhase, String step, OutputTag<Tuple2<String, EsDosage>> outputTag) throws Exception {
        LocalDateTime localDateTime = DateUtil.parseLocalDateTime(esDosagePhase.getData_time()).withSecond(0);

        String key = esDosagePhase.getFeed_id() + DateUtil.toEpochSecond(tupleTime.f0) + DateUtil.toEpochSecond(tupleTime.f1);
        String startTimeValue = cluster.hget(key, PropertiesConstants.START_TIME);
        double value = 0.0;
        String startValue = String.valueOf(esDosagePhase.getData_value());
        String endValue = String.valueOf(esDosagePhase.getData_value());
        if (StringUtils.isEmpty(startTimeValue)) {
            cluster.hset(key, PropertiesConstants.START_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
        } else {
            String[] split = startTimeValue.split("#");
            LocalDateTime startTime = DateUtil.parseLocalDateTime(split[0]);
            //离线处理, 如果数据时间等于开始时间 就重新设置开始时间数据.
            boolean isOffline = localDateTime.equals(startTime) || localDateTime.isBefore(startTime);
            if (isOffline) {
                cluster.hset(key, PropertiesConstants.START_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
            } else {
                startValue = split[1];
            }
        }

        String endTimeValue = cluster.hget(key, PropertiesConstants.END_TIME);
        if (StringUtils.isNotEmpty(endTimeValue)) {
            String[] split = endTimeValue.split("#");
            String time = split[0];
            LocalDateTime endTime = DateUtil.parseLocalDateTime(time);
            //离线数据过来 如果是修改的当前结束时间的数值, 就要更新结束时间的数值, value结果不变,  如果数据不是修改的结束时间数值就不用关系, value 用查出的结束值减去开始值
            boolean isOffline = localDateTime.equals(endTime) || localDateTime.isAfter(endTime);
            if (isOffline) {
                cluster.hset(key, PropertiesConstants.END_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
            } else {
                endValue = split[1];
            }
        } else {
            cluster.hset(key, PropertiesConstants.END_TIME, esDosagePhase.getData_time() + "#" + esDosagePhase.getData_value());
        }

        //计算  开始时间的值 - 结束时间的值
        BigDecimal subtract = new BigDecimal(endValue).subtract(new BigDecimal(startValue));
        value = subtract.doubleValue();


        cluster.expire(key, 35 * 24 * 60 * 60);//设置35天过期

        EsDosage esDosage = new EsDosage(esDosagePhase.getFeed_id(), value,
                DateUtil.toEpochSecond(tupleTime.f0), DateUtil.toEpochSecond(tupleTime.f1), DateUtil.formatLocalDateTime(tupleTime.f1)
                , step, new Date(), new Date());

        ctx.output(outputTag, new Tuple2<>(key, esDosage));
    }

    private void outPutData(MapState<String, Double> curState, Tuple2<LocalDateTime, LocalDateTime> tupleTime, Context ctx, EsDosagePhase esDosagePhase, Double subtract, String step, OutputTag<EsDosage> outputTag) throws Exception {
        String key = DateUtil.formatTime(tupleTime.f0) + "_" + DateUtil.formatTime(tupleTime.f1);
        if (curState.get(key) == null) {
            curState.put(key, 0.0);
        } else {
            Double aDouble = curState.get(key);
            curState.put(key, aDouble + subtract);
        }
        EsDosage esDosage = new EsDosage(esDosagePhase.getFeed_id(), curState.get(key),
                DateUtil.toEpochSecond(tupleTime.f0), DateUtil.toEpochSecond(tupleTime.f1), DateUtil.formatLocalDateTime(tupleTime.f1)
                , step, new Date(), new Date());

        ctx.output(outputTag, esDosage);
    }

}