package com.puchen.util;


import com.puchen.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisUtil {

    private final static JedisPool pool;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "192.168.31.128", 6379,10*1000,"123456");
    }

    public static Jedis getJedis() {
        // Jedis jedis = new Jedis("hadoop102", 6379);

        Jedis jedis = pool.getResource();
        jedis.select(4); // 直接选择 4 号库

        return jedis;
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();  // 如果 jedis 客户端是 new Jedis()得到的,则是关闭客户端.如果是通过连接池得到的,则归还
        }
    }

    public static String getReidsKey(String tableName,String id){
        return Constant.HBASE_NAMESPACE + ":" + tableName + ":" + id;

    }


    /**
     * 获取到 redis 的异步连接
     *
     * @return 异步链接对象
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisURI build = RedisURI.builder()
                .withHost("192.168.31.128")
                .withPort(6379)
                .withTimeout(Duration.ofMillis(5000))
                .withPassword("123456")
                .withDatabase(4)
                .build();
        RedisClient redisClient = RedisClient.create(build);
        return redisClient.connect();
    }

    /**
     * 关闭 redis 的异步连接
     *
     * @param redisAsyncConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }


//    public static void main(String[] args) {
////        StatefulRedisConnection<String, String> redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
////        System.out.println(redisAsyncConnection);
//
//        RedisURI build = RedisURI.builder()
//                .withHost("192.168.31.128")
//                .withPort(6379)
//                .withTimeout(Duration.ofMillis(5000))
//                .withPassword("123456")
//                .withDatabase(4)
//                .build();
//
//        RedisClient redisClient = RedisClient.create(build);
//        System.out.println(redisClient);
//    }

    /**
     * 异步的方式从 redis 读取维度数据
     * @param redisAsyncConn 异步连接
     * @param tableName 表名
     * @param id id 的值
     * @return 读取到维度数据,封装的 json 对象中
     */
//    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
//                                          String tableName,
//                                          String id) {
//        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
//        String key = getKey(tableName, id);
//        try {
//            String json = asyncCommand.get(key).get();
//            if (json != null) {
//                return JSON.parseObject(json);
//            }
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        return null;
//    }

    /**
     * 把维度异步的写入到 redis 中
     * @param redisAsyncConn  到 redis 的异步连接
     * @param tableName 表名
     * @param id id 的值
     * @param dim 要写入的维度数据
     */
//    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
//                                     String tableName,
//                                     String id,
//                                     JSONObject dim) {
//        // 1. 得到异步命令
//        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
//
//        String key = getKey(tableName, id);
//        // 2. 写入到 string 中: 顺便还设置的 ttl
//        asyncCommand.setex(key, Constant.TWO_DAY_SECONDS, dim.toJSONString());
//
//    }
}