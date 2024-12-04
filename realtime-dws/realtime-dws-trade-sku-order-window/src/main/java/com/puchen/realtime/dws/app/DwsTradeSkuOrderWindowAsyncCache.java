package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.TradeSkuOrderBean;
import com.puchen.constant.Constant;
import com.puchen.function.DimAsyncFunction;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import com.puchen.util.HbaseUtil;
import com.puchen.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.api.common.time.Time.seconds;

public class DwsTradeSkuOrderWindowAsyncCache extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowAsyncCache().start(
                10029,
                4,
                "dws_trade_sku_order_window_async_cache",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.读取dwd层下单主题数据
//        stream.print();
        //2.过滤清洗 -> null
        SingleOutputStreamOperator<JSONObject> jsonStream = getJsonStream(stream);
        //3.添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = getTs(jsonStream);
        //4.修正度量值 转换数据结构
        KeyedStream<JSONObject, String> keyedStream = getId(withWaterMarkStream);
        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = getProcess(keyedStream);
//        processStream.print();
        //5.分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = getReduce(processStream);
//        reduceStream.print();

        //6.关联维度信息
        //6.1异步关联sku_info 信息
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream =
                    AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeSkuOrderBean>() {
                        @Override
                        public String getId(TradeSkuOrderBean input) {
                            return input.getSkuId();
                        }

                        @Override
                        public String getTableName() {
                            return "dim_sku_info";
                        }

                        @Override
                        public void join(TradeSkuOrderBean bean, JSONObject dimSkuInfo) {
                            bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                            bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                            bean.setSpuId(dimSkuInfo.getString("spu_id"));
                            bean.setSkuName(dimSkuInfo.getString("sku_name"));
                        }
                    }, 60L, TimeUnit.SECONDS);


        //6.2异步关联spu_info 信息
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            public String getTableName() {
                return "dim_spu_info";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject jsonObject) {
                input.setSpuName(jsonObject.getString("spu_name"));
            }
        }, 60L, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> trademarkStream = AsyncDataStream.unorderedWait(spuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject jsonObject) {
                input.setTrademarkName(jsonObject.getString("tm_name"));
            }
        }, 60L, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(trademarkStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject jsonObject) {
                input.setCategory3Name(jsonObject.getString("name"));
                input.setCategory2Id(jsonObject.getString("category2_id"));
            }
        }, 60L, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void join(TradeSkuOrderBean bean, JSONObject dimBaseCategory2) {
                bean.setCategory2Name(dimBaseCategory2.getString("name"));
                bean.setCategory1Id(dimBaseCategory2.getString("category1_id"));
            }
        }, 60L, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeSkuOrderBean> fullStreamStream = AsyncDataStream.unorderedWait(c2Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void join(TradeSkuOrderBean bean, JSONObject dimBaseCategory2) {
                bean.setCategory1Name(dimBaseCategory2.getString("name"));
            }
        }, 60L, TimeUnit.SECONDS);
//        fullStreamStream.print();

        //7.写出到doris
        fullStreamStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));

    }


    public static KeyedStream<JSONObject, String> getId(SingleOutputStreamOperator<JSONObject> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> jsonStream) {
        return jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> getMap(SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream) {
        return reduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            Connection connection;
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HbaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeConnection(connection);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                //(1)使用hbase的api读取表格数据 get
                JSONObject dimSkuInfo = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", bean.getSkuId());

                //(2)使用读取到的字段补全原本的信息
                bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                bean.setSpuId(dimSkuInfo.getString("spu_id"));
                bean.setSkuName(dimSkuInfo.getString("sku_name"));

                //继续关联别的维度表
                //关联spu表
                JSONObject dimSpuInfo = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_spu_info", bean.getSpuId());
                bean.setSpuName(dimSpuInfo.getString("spu_name"));

                //关联dim_base_category3
                JSONObject dimBaseCategory3 = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category3", bean.getCategory3Id());
                bean.setCategory3Name(dimBaseCategory3.getString("name"));
                bean.setCategory2Id(dimBaseCategory3.getString("category2_id"));

                //关联c2表
                JSONObject dimBaseCategory2 = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category2", bean.getCategory2Id());
                bean.setCategory2Name(dimBaseCategory2.getString("name"));
                bean.setCategory1Id(dimBaseCategory2.getString("category1_id"));

                //关联c1表
                JSONObject dimBaseCategory1 = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category1", bean.getCategory1Id());
                bean.setCategory1Name(dimBaseCategory1.getString("name"));

                //关联品牌表
                JSONObject dimTrademark = HbaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_trademark", bean.getTrademarkId());
                bean.setTrademarkName(dimTrademark.getString("tm_name"));
                return bean;
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> getJsonStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                try {
                    if (value != null) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        Long ts = jsonObject.getLong("ts");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        if (ts != null && id != null && skuId != null) {
                            jsonObject.put("ts", ts * 1000);
                            out.collect(jsonObject);
                        }
                    }

                } catch (Exception e) {
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> getReduce(SingleOutputStreamOperator<TradeSkuOrderBean> processStream) {
        return processStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean value) throws Exception {
                        return value.getSkuId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeSkuOrderBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }
                    }
                });
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> getProcess(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> out) throws Exception {
                //调取状态中的度量值
//                // 原始金额
//                BigDecimal originalAmount;
//                // 活动减免金额
//                BigDecimal activityReduceAmount;
//                // 优惠券减免金额
//                BigDecimal couponReduceAmount;
//                // 下单金额
//                BigDecimal orderAmount;
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");
//测试
//                if(orderAmount != null){
//                    System.out.println("相同的id"+jsonObject);
//                }

                originalAmount = originalAmount == null ? BigDecimal.ZERO : orderAmount;
                activityReduceAmount = activityReduceAmount == null ? BigDecimal.ZERO : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? BigDecimal.ZERO : couponReduceAmount;
                orderAmount = orderAmount == null ? BigDecimal.ZERO : orderAmount;

                BigDecimal curOriginAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));

                //每一条相同id的数据 度量值减去上一条状态中的数据值
                TradeSkuOrderBean build = TradeSkuOrderBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginAmount.subtract(originalAmount))
                        .orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();

                lastAmountState.put("originalAmount",curOriginAmount);
                lastAmountState.put("activityReduceAmount",jsonObject.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount",jsonObject.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount",jsonObject.getBigDecimal("split_total_amount"));
                out.collect(build);
            }
        });
    }
}
