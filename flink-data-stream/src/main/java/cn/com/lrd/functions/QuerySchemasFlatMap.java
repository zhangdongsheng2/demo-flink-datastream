package cn.com.lrd.functions;

import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.CreateMySqlPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @description: 查询Schema 查不到的过滤掉
 * @author: zhangdongsheng
 * @date: 2020/5/9 09:06
 */
@Slf4j
public class QuerySchemasFlatMap extends RichFlatMapFunction<InputDataSingle, InputDataSingle> {
    private Connection con;
    private PreparedStatement ps;
    private Map<String, String> dsSchemas;

    @Override
    public void open(Configuration parameters) throws Exception {
        con = CreateMySqlPool.getConnection();
        String sql = "SELECT ds_schema,dev_seri_no,prod_key FROM " + "iothub_dev" + ".key_schema where dev_seri_no=?;";
        ps = con.prepareStatement(sql);
        dsSchemas = new HashMap<>();
    }

    @Override
    public void flatMap(InputDataSingle value, Collector<InputDataSingle> out) throws Exception {
        String schema = dsSchemas.get(value.getSn());
        if (StringUtils.isEmpty(schema)) {
            ps.setString(1, value.getSn());
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                schema = resultSet.getString("ds_schema");
                dsSchemas.put(value.getSn(), schema);
            }
        }
        if (StringUtils.isNotEmpty(schema)) {
            value.setDsSchema(schema);
            out.collect(value);
        } else {
            log.trace("keyToSchema error!");
        }
    }

    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (con != null) {
            con.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}