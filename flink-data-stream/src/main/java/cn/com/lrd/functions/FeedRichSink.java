package cn.com.lrd.functions;

import cn.com.lrd.utils.ParameterToolUtil;
import com.commerce.commons.constant.PropertiesConstants;
import com.commerce.commons.enumeration.*;
import com.commerce.commons.model.InputDataSingle;
import com.commerce.commons.utils.CreateMySqlPool;
import com.commerce.commons.utils.DateUtil;
import com.commerce.commons.utils.JedisClusterUtil;
import com.commerce.commons.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisCluster;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * @description: 生成仪表到 MySQL
 * 可优化: 查询Schemas 可以合并到这里;  如果需要这里可加事务. 用二阶段提交;  去重阶段Redis可以去掉;
 * @author: zhangdongsheng
 * @date: 2020/5/9 08:58
 */
public class FeedRichSink extends RichSinkFunction<InputDataSingle> {
    private transient Connection con;
    private transient Statement statement;
    private transient JedisCluster cluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        con = CreateMySqlPool.getConnection();
        statement = con.createStatement();
        cluster = JedisClusterUtil.getJedisCluster(ParameterToolUtil.getParameterTool());
    }

    @Override
    public void invoke(InputDataSingle value, Context context) throws Exception {
        //查询设备是否存在
        String iotDeviceSql = " SELECT id,dev_seri_no FROM " + value.getDsSchema() + ".iot_device WHERE dev_seri_no=" + value.getSn() + " AND is_del=0";
        ResultSet iotDeviceResultSet = statement.executeQuery(iotDeviceSql);
        String iotDeviceId = "";
        String iotDevSeriNo = "";
        if (iotDeviceResultSet.next()) {
            //存在
            iotDeviceId = iotDeviceResultSet.getString("id");
            iotDevSeriNo = iotDeviceResultSet.getString("dev_seri_no");
        }
        if (StringUtils.isEmpty(iotDeviceId)) return;


        //查询Topic是否存在
        String topicSql = " SELECT id FROM " + value.getDsSchema() + ".topic WHERE device_id='" + iotDeviceId + "' AND value='" + value.getTopic() + "' AND is_del=0";
        ResultSet topicResultSet = statement.executeQuery(topicSql);
        String topicId = "";
        if (topicResultSet.next()) {
            topicId = topicResultSet.getString("id");
            //存在
        } else {
            return;
        }

        //查询仪表是否存在, 不存在就创建
        String instrumentQuerySql = "SELECT id FROM " + value.getDsSchema() + ".instrument WHERE device_id='" + iotDeviceId + "' AND addr='" + value.getAdd() + "' AND type=" + value.getType();
        ResultSet instrumentResultSet = statement.executeQuery(instrumentQuerySql);
        String instrumentId;
        if (instrumentResultSet.next()) {//未查到进行创建
            instrumentId = instrumentResultSet.getString("id");
        } else {
            instrumentId = UUID.randomUUID().toString();
            String instrumentInsertSql = "INSERT INTO  " + value.getDsSchema() + ".instrument(id, create_time, modify_time, version, addr, device_id,  topic_id, type) VALUES ('"
                    + instrumentId + "','" + DateUtil.getCurrentDateTimeStr() + "','" + DateUtil.getCurrentDateTimeStr() + "'," + 0 + ","
                    + value.getAdd() + ",'" + iotDeviceId + "','" + topicId + "'," + value.getType()
                    + ")";
            statement.executeUpdate(instrumentInsertSql);
        }

        //使用UUID生成inputId和feedId
        String inputId = UUID.randomUUID().toString().replaceAll("-", "");
        String feedId = UUID.randomUUID().toString().replaceAll("-", "");
        String mapValue = inputId + "," + feedId;
        String feedInputMapKey = value.getSn() + "_"
                + value.getCode() + "_"
                + value.getType() + "_"
                + value.getAdd();
        cluster.hset(PropertiesConstants.LARUNDA_INPUT_FEED_KEY, feedInputMapKey, mapValue);

        //查询input  feed 是否存在不存在就创建
        String inputQuerySql = "SELECT id FROM " + value.getDsSchema() + ".input WHERE inst_id='" + instrumentId + "' AND prop='" + value.getCode() + "' AND inst_addr=" + value.getAdd() + " AND inst_type=" + value.getType();
        ResultSet inputResultSet = statement.executeQuery(inputQuerySql);
        if (!inputResultSet.next()) {
            String inputInsertSql = "INSERT INTO  " + value.getDsSchema() + ".input(id, create_time, modify_time, version, prop, value, value_time, inst_addr, inst_type, inst_id) VALUES ('"
                    + inputId + "','" + DateUtil.getCurrentDateTimeStr() + "','" + DateUtil.getCurrentDateTimeStr() + "'," + 0 + ","
                    + "'" + value.getCode() + "'," + value.getValue() + ",'" + value.getTime() + "', " + value.getAdd() + "," + value.getType() + ",'" + instrumentId
                    + "')";
            statement.executeUpdate(inputInsertSql);

            String currentNumSql = "SELECT max(num) as max_num FROM " + value.getDsSchema() + ".feed WHERE device_id = '" + iotDeviceId + "' and feed_type = 'NORMAL'";
            ResultSet currentNumResultSet = statement.executeQuery(currentNumSql);
            int currentNum = 0;
            if (currentNumResultSet.next()) {
                currentNum = currentNumResultSet.getInt("max_num");
            }
            currentNum = currentNum + 1;
            //生成feed编号：设备SN+五位编号（从1开始，不够前补0）
            String feedNum = String.format(value.getSn() + "%05d", currentNum);
            //name([属性代码]_[feedId])
            String name = value.getCode() + "_" + feedId;
            String feedInsertSql = "INSERT INTO  " + value.getDsSchema() + ".feed(id, create_time, modify_time, version, device_id, feed_num, num, name, last_value, update_time, input_id, " +
                    "feed_type, is_auto, interval_value, interval_unit_type, feed_value_type, feed_log_value_type, inst_id, is_default) VALUES ('"
                    + feedId + "','" + DateUtil.getCurrentDateTimeStr() + "','" + DateUtil.getCurrentDateTimeStr() + "'," + 0 + ","
                    + "'" + iotDeviceId + "','" + feedNum + "'," + currentNum + ", '" + name + "'," + value.getValue() + ",'" + value.getTime() + "','" + inputId + "','"
                    + FeedType.NORMAL + "'," + true + "," + 1 + ",'" + IntervalUnitType.DAY + "','" + FeedValueType.ORI + "','" + FeedLogValueType.INSTANT + "','" + instrumentId + "'," + true
                    + ")";

            statement.executeUpdate(feedInsertSql);
        }


        //查询Bsn 是否存在 , 不存在就创建
        String iotBsnQuerySql = "SELECT id FROM " + value.getDsSchema() + ".iot_bsn WHERE inst_id='" + instrumentId + "' AND status='" + BsnStatusEnum.VALID + "'";
        ResultSet iotBsnResultSet = statement.executeQuery(iotBsnQuerySql);
        if (!iotBsnResultSet.next()) {
            String s = value.getSn() + "a" + value.getAdd() + "t" + value.getType();
            String bsn = StringUtil.convert(StringUtil.str2HexStr(s));
            String iotBsnInsertSql = "INSERT INTO  " + value.getDsSchema() + ".iot_bsn(id, create_time, modify_time, version, bsn, status, valid_date, remarks, inst_id) VALUES ('"
                    + UUID.randomUUID().toString() + "','" + DateUtil.getCurrentDateTimeStr() + "','" + DateUtil.getCurrentDateTimeStr() + "'," + 0 + ","
                    + "'" + bsn + "','" + BsnStatusEnum.VALID + "','" + LocalDateTime.now() + "', 'new', '" + instrumentId
                    + "')";
            statement.executeUpdate(iotBsnInsertSql);


            String bsnToSchemaInsertSql = "INSERT INTO  iothub_dev.bsn_schema(id, create_time, modify_time, version, ds_schema, bsn) VALUES ('"
                    + UUID.randomUUID().toString() + "','" + DateUtil.getCurrentDateTimeStr() + "','" + DateUtil.getCurrentDateTimeStr() + "'," + 0 + ","
                    + "'" + value.getDsSchema() + "','" + bsn
                    + "')";
            statement.executeUpdate(bsnToSchemaInsertSql);
        }

    }

    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (con != null) {
            con.close();
        }
        if (statement != null) {
            statement.close();
        }
    }
}
