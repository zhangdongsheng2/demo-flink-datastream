package com.commerce.commons.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/7 18:12
 */
public class JedisClusterUtil {
    private static final Log logger = LogFactory.getLog(JedisClusterUtil.class);

    private static Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
    private static JedisCluster jedisCluster = null;

    /**
     * 初始化jedisCluster对象
     */
    static {
        try {
            jedisCluster = reloadJedisCluster();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 集群模式
     * 获取JedisCluster对象
     *
     * @return
     * @throws Exception
     */
    public static JedisCluster getJedisCluster() throws Exception {
        if (jedisCluster == null) {
            synchronized (JedisClusterUtil.class) {
                jedisCluster = reloadJedisCluster();
            }
        }
        return jedisCluster;
    }

    /**
     * 集群模式
     * 获取JedisCluster方法
     *
     * @return
     * @throws Exception
     */
    public static JedisCluster reloadJedisCluster() throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        // 最大连接数
        config.setMaxTotal(1000);
        // 最大连接空闲数
        config.setMaxIdle(1000);
        //设置集群状态扫描间隔
        config.setTimeBetweenEvictionRunsMillis(200000);
        //最大建立连接等待时间
        config.setMaxWaitMillis(10000);
        //在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(true);
        //在空闲时检查有效性, 默认false
        config.setTestOnReturn(true);

        logger.info("初始化实体");
        JedisCluster cluster;
        String redisAddrCfg = ExecutionEnvUtil.getParameterTool().get("redis.address");
        logger.info("******redis集群配置：" + redisAddrCfg);
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
        return cluster;
    }


    public static Set<InetSocketAddress> getJedisNodes() throws Exception {
        Set<InetSocketAddress> jedisClusterNodes = new HashSet<InetSocketAddress>();
        String redisAddrCfg = ExecutionEnvUtil.getParameterTool().get("redis.address");
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


    /*
     * 测试
     */
//    public static void main(String[] args) throws Exception {
//        getJedisCluster().hgetAll("dev2-emq-input-feed-id");
//    }
}
