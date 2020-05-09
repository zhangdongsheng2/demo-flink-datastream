package com.commerce.commons.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/7 18:14
 */
public class JedisUtil {
    private static final Log logger = LogFactory.getLog(JedisUtil.class);
    private static Jedis jedis = null;

    /**
     * 初始化jedis对象
     */
    static {
        try {
            jedis = reloadJedis();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 单机模式
     * 获取Jedis对象
     *
     * @return
     * @throws Exception
     */
    public static Jedis getJedis() throws Exception {
        if (jedis == null) {
            synchronized (JedisUtil.class) {
                jedis = reloadJedis();
            }
        }
        return jedis;
    }

    /**
     * 单机模式
     * 获取Jedis方法
     *
     * @return
     * @throws Exception
     */
    public static Jedis reloadJedis() throws Exception {
        logger.info("初始化实体");
        Jedis jedis;
        String redisAddrCfg = ExecutionEnvUtil.getParameterTool().get("redis.address");
        logger.info("******redis单机配置：" + redisAddrCfg);
        if (StringUtils.isEmpty(redisAddrCfg) || redisAddrCfg.split(",").length == 0) {
            throw new Exception("System.properties中REDIS_ADDR_CFG属性为空");
        }
        String[] ipAndPort = redisAddrCfg.split(":");
        if (ipAndPort == null || (ipAndPort.length != 2 && ipAndPort.length != 3)) {
            throw new Exception("System.properties中REDIS_ADDR_CFG属性配置错误");
        }
        jedis = new Jedis(ipAndPort[0], Integer.parseInt(ipAndPort[1]), 2000, 30);
        //如果使用这redis设置有密码，则需要再次设置授权。我的redis配置格式：ip：port:password
        //使用者可以根据自己的情况自行调整
        if (ipAndPort.length == 3) {
            jedis.auth(ipAndPort[2]);
        }
        return jedis;
    }

//    /*
//     * 测试
//     */
//    public static void main(String[] args) throws Exception {
//        getJedis().setex("name", 2, "wangpenghui");
//        System.out.println(getJedis().get("name"));
//        Thread.sleep(3000);
//        System.out.println(getJedis().get("name"));
//    }
}
