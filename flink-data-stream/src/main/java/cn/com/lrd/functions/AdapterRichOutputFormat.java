package cn.com.lrd.functions;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/13 18:45
 */
public abstract class AdapterRichOutputFormat<T> extends RichOutputFormat<T> {
    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public abstract void writeRecord(T record);

    @Override
    public void close() throws IOException {

    }
}
