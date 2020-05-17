package com.commerce.commons.utils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/13 18:21
 */
@Slf4j
public class MyEsUtil {
    private static String ES_HOST = "http://172.16.24.31";
    private static int ES_HTTP_PORT = 9200;
    private static JestClientFactory factory = null;


    /**
     * 获取客户端
     *
     * @return jestclient
     */
    public static JestClient getJestClient() {
        if (factory == null) build();
        return factory.getObject();
    }


    /**
     * 建立连接
     */
    private static void build() {
        factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
                .maxTotalConnection(20) //连接总数
                .connTimeout(10000).readTimeout(10000).build());
    }


    /**
     * 关闭客户端
     */
    public static void close(JestClient client) {
        if (!Objects.isNull(client)) {
            client.shutdownClient();
        }
    }


    public static void executeIndexBulk(String indexName, Iterable<Tuple2<String, Object>> list) throws Exception {
        JestClient jestClient = getJestClient();
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc");
        for (Tuple2<String, Object> stringObjectTuple2 : list) {
            Object doc = stringObjectTuple2.f1;
            Index.Builder indexBuilder = new Index.Builder(doc);
            if (StringUtils.isNotEmpty(stringObjectTuple2.f0)) {
                indexBuilder.id(stringObjectTuple2.f0);
            }
            Index index = indexBuilder.build();

            bulkBuilder.addAction(index);
        }
        BulkResult result = jestClient.execute(bulkBuilder.build());
        log.info("已保存: {} 条数据", result.getItems().size());
        close(jestClient);

        /// bulk == batch
        //  val index: Index = new Index.Builder(json).index("gmall0808_dau").`type`("_doc").id("3").build()
    }


}
