package com.puchen.function;

import com.alibaba.fastjson.JSONObject;
import com.puchen.bean.TradeSkuOrderBean;
import com.puchen.constant.Constant;
import com.puchen.util.HbaseUtil;
import com.puchen.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    String tableName = getTableName();

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HbaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HbaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //java的异步编程方式
        String rowKey = getId(input);
        String reidsKey = RedisUtil.getReidsKey(tableName, rowKey);
        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                // 第一步 异步访问得到的数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(reidsKey);

                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                }catch (Exception e){
                    e.printStackTrace();
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJson = null;
                //旁路缓存判断
                if (dimInfo == null || dimInfo.isEmpty()){
                    //reids没有读取到数据 需要访问hbase
                    dimJson = HbaseUtil.readDimAsync(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                    //将读取的数据保存到redis
                    redisAsyncConnection.async().setex(reidsKey,24*60*60,dimJson.toJSONString());
                }else {
                    dimJson = JSONObject.parseObject(dimInfo);
                }
                return dimJson;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                if (jsonObject == null){
                    System.out.println("无法关联到维度信息"+tableName+rowKey);
                }else {
                    join(input,jsonObject);
                }
//                        resultFuture.complete(Arrays.asList(input));
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }


}
