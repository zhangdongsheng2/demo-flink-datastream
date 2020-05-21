package cn.com.lrd.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @description: Jedis 工具类, 防止因为 redis报错导致程序挂掉, 这里做了递归, 如果报错就重新初始化, 直到不报错.
 * @author: zhangdongsheng
 * @date: 2020/5/7 18:12
 */
@Slf4j
public class JedisClusterUtil {
    private static JedisCluster jedisCluster = null;

    /**
     * 集群模式
     * 获取JedisCluster对象
     */
    public static JedisCluster getJedisCluster() throws Exception {
        if (jedisCluster == null) {
            synchronized (JedisClusterUtil.class) {
                reloadJedisCluster();
            }
        }
        return jedisCluster;
    }

    /**
     * 集群模式
     * 获取JedisCluster方法
     * 初始化jedisCluster对象
     *
     * @return
     * @throws Exception
     */
    public static void reloadJedisCluster() throws Exception {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        JedisPoolConfig config = new JedisPoolConfig();
        // 最大连接数
        config.setMaxTotal(200);
        // 最大连接空闲数
        config.setMaxIdle(100);
        //设置集群状态扫描间隔
        config.setTimeBetweenEvictionRunsMillis(200000);
        //最大建立连接等待时间
        config.setMaxWaitMillis(10000);
        //在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(true);
        //在空闲时检查有效性, 默认false
        config.setTestOnReturn(true);

        log.info("初始化实体");
        JedisCluster cluster;
        String redisAddrCfg = ParameterToolUtil.getParameterTool().get("redis.address");
        log.info("******redis集群配置：" + redisAddrCfg);
        if (StringUtils.isEmpty(redisAddrCfg) || redisAddrCfg.split(",").length == 0) {
            throw new Exception("System.properties中REDIS_ADDR_CFG属性为空");
        }
        String[] addrs = redisAddrCfg.split(",");
        for (String addr : addrs) {
            String[] ipAndPort = addr.split(":");
            if (ipAndPort.length != 2) {
                throw new Exception("System.properties中REDIS_ADDR_CFG属性配置错误");
            }

            jedisClusterNodes.add(new HostAndPort(ipAndPort[0],
                    Integer.parseInt(ipAndPort[1])));
        }
        cluster = new JedisCluster(jedisClusterNodes, 30000, 3000, 10, config);
        jedisCluster = cluster;
    }


    public static Set<InetSocketAddress> getJedisNodes(ParameterTool parameterTool) throws Exception {
        Set<InetSocketAddress> jedisClusterNodes = new HashSet<InetSocketAddress>();
        String redisAddrCfg = parameterTool.get("redis.address");
        if (StringUtils.isEmpty(redisAddrCfg) || redisAddrCfg.split(",").length == 0) {
            throw new Exception("System.properties中REDIS_ADDR_CFG属性为空");
        }
        String[] addrs = redisAddrCfg.split(",");
        for (String addr : addrs) {
            String[] ipAndPort = addr.split(":");
            if (ipAndPort.length != 2) {
                throw new Exception("System.properties中REDIS_ADDR_CFG属性配置错误");
            }
            jedisClusterNodes.add(new InetSocketAddress(ipAndPort[0],
                    Integer.parseInt(ipAndPort[1])));
        }
        return jedisClusterNodes;
    }


    public static boolean hexists(String key, String field) {
        final boolean[] value = new boolean[1];
        jedisRunMethod(jedisCluster -> value[0] = jedisCluster.hexists(key, field));
        return value[0];
    }


    public static void hset(String key, String field, String value) {
        jedisRunMethod(jedisCluster -> jedisCluster.hset(key, field, value));
    }


    public static String hget(String key, String field) {
        final String[] value = {""};
        jedisRunMethod(jedisCluster -> value[0] = jedisCluster.hget(key, field));
        return value[0];
    }

    public static Map<String, String> hgetAll(String key) {
        Map<String, String> value = new HashMap<>();
        jedisRunMethod(jedisCluster -> value.putAll(jedisCluster.hgetAll(key)));
        return value;
    }


    public static void expire(String key, int seconds) {
        jedisRunMethod(jedisCluster -> jedisCluster.expire(key, seconds));
    }


    public static void del(String key) {
        jedisRunMethod(jedisCluster -> jedisCluster.del(key));
    }


    //间隔1秒 递归防止报错
    public static void jedisRunMethod(JedisMethod jedisMethod) {
        try {
            jedisMethod.run(getJedisCluster());
        } catch (Exception e) {
            log.info("JedisCluster 报错 重新获取 递归调用 jedisRunMethod <<<< {}", e.getMessage());
            try {
                reloadJedisCluster();
                Thread.sleep(1000);
                jedisRunMethod(jedisMethod);
            } catch (Exception ex) {
                log.info("JedisCluster 报错 重新获取 reloadJedisCluster失败 递归调用 jedisRunMethod =====  <<<< {}", ex.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException exc) {
                    exc.printStackTrace();
                }
                jedisRunMethod(jedisMethod);
            }
        }
    }

    interface JedisMethod {
        void run(JedisCluster jedisCluster);
    }


}
