package com.commerce.commons.utils;

import com.commerce.commons.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

public class ESSinkUtil {
    /**
     * es sink 创建, 并设置失败Handler, 和多少条刷新数据到ES
     *
     * @param data 数据
     */
    public static <T> void addSink(int parallelism, DataStream<T> data, ElasticsearchSinkFunction<T> func, ParameterTool parameterTool) {
        ElasticsearchSink.Builder<T> esSinkBuilder =
                new ElasticsearchSink.Builder<>(ESSinkUtil.getEsAddresses(parameterTool.get(PropertiesConstants.ELASTICSEARCH_HOSTS)), func);
        esSinkBuilder.setBulkFlushMaxActions(2);
        esSinkBuilder.setFailureHandler(new RetryRequestFailureHandler());
        //xpack security
        data.addSink(esSinkBuilder.build()).name("ESSink").setParallelism(parallelism);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
//            if (host.startsWith("http")) {
//                URL url = new URL(host);
//                addresses.add(new HttpHost(url.getHost(), url.getPort()));
//            } else {
            String[] parts = host.split(":", 2);
            if (parts.length > 1) {
                addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
//                } else {
//                    throw new MalformedURLException("invalid elasticsearch hosts format");
//                }
            }
        }
        return addresses;
    }
}
