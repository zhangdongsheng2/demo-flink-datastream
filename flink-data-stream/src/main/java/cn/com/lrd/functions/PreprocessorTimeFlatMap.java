package cn.com.lrd.functions;

import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @description: 数据添加数据所属时间阶段
 * @author: zhangdongsheng
 * @date: 2020/5/13 15:30
 */
public class PreprocessorTimeFlatMap implements FlatMapFunction<InputDataSingle, Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>> {

    @Override
    public void flatMap(InputDataSingle value, Collector<Tuple6<String, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, Tuple2<LocalDateTime, LocalDateTime>, InputDataSingle>> out) throws Exception {
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
        LocalDateTime monthStartTime = LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), 1, 0, 0);
        LocalDateTime monthEndTime = monthStartTime.plusMonths(1);


        if (minute == 0 || minute == 30) {
            //01:00 加一条 00:30-01:00     01:30 加一条 01:00 - 01:30
            LocalDateTime halfEndTime2 = halfStartTime;
            LocalDateTime halfStartTime2 = halfEndTime2.minusMinutes(30);

            LocalDateTime hourEndTime2 = hourEndTime;
            LocalDateTime hourStartTime2 = hourStartTime;

            LocalDateTime dayEndTime2 = dayEndTime;
            LocalDateTime dayStartTime2 = dayStartTime;

            LocalDateTime monthEndTime2 = monthEndTime;
            LocalDateTime monthStartTime2 = monthStartTime;
            // 02:00 的数据设置为 01:00-02:00
            if (minute == 0) {
                hourEndTime2 = hourStartTime;
                hourStartTime2 = localDateTime.minusHours(1);
            }
            //day 数据处理, 2020-5-5 00:00  的数据加一条 2020-5-4 00:00 -- 2020-5-5 00:00
            if (localDateTime.getHour() == 0 && minute == 0) {
                dayEndTime2 = dayStartTime;
                dayStartTime2 = dayStartTime.minusDays(1);
            }
            //month 数据处理, 2020-6-1 00:00  的数据加一条 2020-6-1 00:00 -- 2020-7-1 00:00
            if (localDateTime.getDayOfMonth() == 1 && localDateTime.getHour() == 0 && minute == 0) {
                monthEndTime2 = monthStartTime;
                monthStartTime2 = monthStartTime.minusMonths(1);
            }
            out.collect(new Tuple6<>(value.getFeedId(), new Tuple2<>(halfStartTime2, halfEndTime2), new Tuple2<>(hourStartTime2, hourEndTime2), new Tuple2<>(dayStartTime2, dayEndTime2), new Tuple2<>(monthStartTime2, monthEndTime2), value));
        }


        out.collect(new Tuple6<>(value.getFeedId(), new Tuple2<>(halfStartTime, halfEndTime), new Tuple2<>(hourStartTime, hourEndTime), new Tuple2<>(dayStartTime, dayEndTime), new Tuple2<>(monthStartTime, monthEndTime), value));
    }
}


