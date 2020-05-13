package cn.com.lrd.functions;

import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @description: 数据添加时间阶段
 * @author: zhangdongsheng
 * @date: 2020/5/13 15:30
 */
public class PreprocessorTimeMap implements MapFunction<InputDataSingle, Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>> {
    @Override
    public Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle> map(InputDataSingle value) throws Exception {
        String time = value.getTime();
        LocalDateTime localDateTime = DateUtil.parseLocalDateTime(time);
        //time 属于哪个半小时  开始时间结束时间
        int minute = localDateTime.getMinute();
        LocalDateTime halfStartTime = LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(), localDateTime.getHour(), minute < 30 ? 0 : 30, 0);
        LocalDateTime halfEndTime = halfStartTime.plusMinutes(30);
        //time 属于哪个一小时
        LocalDateTime hourStartTime = LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(), localDateTime.getHour(), 0, 0);
        LocalDateTime hourEndTime = hourStartTime.plusHours(1);
        //time 属于哪个天
        LocalDateTime dayStartTime = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.MIN);
        LocalDateTime dayEndTime = dayStartTime.plusDays(1);
        //time 属于哪个月
        LocalDateTime monthStartTime = LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), 0, 0, 0);
        LocalDateTime monthEndTime = monthStartTime.plusMonths(1);

        return new Tuple6<>(value.getFeedId(), new Tuple2<>(halfStartTime, halfEndTime), new Tuple2<>(hourStartTime, hourEndTime), new Tuple2<>(dayStartTime, dayEndTime), new Tuple2<>(monthStartTime, monthEndTime), value);
    }
}