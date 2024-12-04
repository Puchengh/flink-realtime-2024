package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.TradeProvinceOrderBean;
import com.puchen.constant.Constant;
import com.puchen.function.DimAsyncFunction;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10030,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

//        stream.print();

        //1.清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonStream = getJsonStream(stream);
        //2.添加水位线
        SingleOutputStreamOperator<JSONObject> tsStream = getTs(jsonStream);
        //3.度量值去重 转换为javabean
        KeyedStream<JSONObject, String> keyedStream = getId(tsStream);
        //4.分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> lastToalAmountStream = getLastToalAmount(keyedStream);

        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream = getReduce(lastToalAmountStream);
//        reduceStream.print();
        //5.补全维度信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceStream = getName(reduceStream);
//        provinceStream.print();
        //6.写出到doris
        provinceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }

    public static SingleOutputStreamOperator<TradeProvinceOrderBean> getName(SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream) {
        return AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getId(TradeProvinceOrderBean input) {
                return input.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean input, JSONObject jsonObject) {
                input.setProvinceName(jsonObject.getString("name"));
            }
        }, 30, TimeUnit.SECONDS);
    }

    public static SingleOutputStreamOperator<TradeProvinceOrderBean> getReduce(SingleOutputStreamOperator<TradeProvinceOrderBean> lastToalAmountStream) {
        return lastToalAmountStream.keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderBean tradeProvinceOrderBean) throws Exception {
                        return tradeProvinceOrderBean.getProvinceId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeProvinceOrderBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            element.setOrderCount((long) element.getOrderIdSet().size());
                            collector.collect(element);
                        }
                    }
                });
    }

    public static SingleOutputStreamOperator<TradeProvinceOrderBean> getLastToalAmount(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {

            ValueState<BigDecimal> lastToalAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastToalAmount = new ValueStateDescriptor<>("last_toal_amount", BigDecimal.class);
                lastToalAmount.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastToalAmountState = getRuntimeContext().getState(lastToalAmount);
            }


            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getString("order_id"));

                BigDecimal amount = lastToalAmountState.value();
//                amount = amount == null ? BigDecimal.ZERO : amount;
                amount = amount == null ? new BigDecimal("0") : amount;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");

                lastToalAmountState.update(splitTotalAmount);
                return TradeProvinceOrderBean.builder()
                        .orderIdSet(hashSet)
                        .provinceId(jsonObject.getString("province_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .orderAmount(splitTotalAmount.subtract(amount))
                        .build();
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> jsonStream) {
        return jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        );
    }

    public static KeyedStream<JSONObject, String> getId(SingleOutputStreamOperator<JSONObject> jsonStream) {
        return jsonStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> getJsonStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                try {
                    if (!value.isEmpty()) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String id = jsonObject.getString("id");
                        String orderId = jsonObject.getString("order_id");
                        String provinceId = jsonObject.getString("province_id");
                        Long ts = jsonObject.getLong("ts");
                        if (id != null && orderId != null && provinceId != null && ts != null) {
                            jsonObject.put("ts", ts * 1000);
                            out.collect(jsonObject);
                        }

                    }

                } catch (Exception e) {
                    System.out.println("过滤脏数据" + value);
                }

            }
        });
    }
}
