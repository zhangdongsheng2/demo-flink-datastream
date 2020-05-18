package cn.com.lrd.offline;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/17 19:11
 */
public class TestDataSet {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<String> input = env.fromElements(
                "Hello",
                "zhisheng",
                "aaaaaaaaa");


        input.print();
    }
}
