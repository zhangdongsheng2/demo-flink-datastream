package cn.com.lrd.functions;

import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.enumeration.PropEnum;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.DateUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: 按照时间过滤数据, 整10分钟的留下
 * @author: zhangdongsheng
 * @date: 2020/5/13 15:30
 */
public class PreprocessorFilterFlatMap extends RichFlatMapFunction<Tuple2<String, InputDataSingle>, InputDataSingle> {
    private List<String> propList;
    private List<String> time_step;

    @Override
    public void open(Configuration parameters) throws Exception {
        time_step = new ArrayList<>(Arrays.asList(PropertiesConstants.TIME_10,
                PropertiesConstants.TIME_20, PropertiesConstants.TIME_30, PropertiesConstants.TIME_40, PropertiesConstants.TIME_50));
        propList = Arrays.stream(PropEnum.values()).map(PropEnum::getCode).collect(Collectors.toList());
    }

    @Override
    public void flatMap(Tuple2<String, InputDataSingle> value, Collector<InputDataSingle> out) throws Exception {
        InputDataSingle originalInput = value.f1;
        String inputTime = originalInput.getTime();
        long time = DateUtil.parseStrDateTime(inputTime);
        String lastStr = inputTime.substring(inputTime.length() - 5);

        //整点或10分钟步发送到
        if (propList.contains(originalInput.getCode())) {
            if (time % PropertiesConstants.LONG_36 == 0 || time_step.contains(lastStr)) {
                out.collect(originalInput);
            }
        }
    }
}
