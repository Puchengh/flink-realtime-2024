package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.CartAddUuBean;
import com.puchen.constant.Constant;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddUuWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                10026,
                4,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {


//        stream.print();
        //1.读取dwd中加购的主题数据
        //2.清洗过滤数据
        SingleOutputStreamOperator<JSONObject> jsonStream = getJsonStream(stream);
        //3.添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterStream = getTs(jsonStream);
        //4.按照user_id进行分组
        KeyedStream<JSONObject, String> keyedStream = getUserId(withWaterStream);
        //5.判断是否为独立用户
        SingleOutputStreamOperator<CartAddUuBean> lastLoginDtStream = getLastLoginDt(keyedStream);
        //6.开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceStream = getReduce(lastLoginDtStream);

//        reduceStream.print();
        //7.写出到doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));
    }

    public static SingleOutputStreamOperator<CartAddUuBean> getReduce(SingleOutputStreamOperator<CartAddUuBean> lastLoginDtStream) {
        return lastLoginDtStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (CartAddUuBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }
                    }
                });
    }

    public static SingleOutputStreamOperator<CartAddUuBean> getLastLoginDt(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

            ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last_login_dt", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastLoginState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {

                //比较前档数据的时间和状态中的上次登录的时间
                String curDt = DateFormatUtil.tsToDate(value.getLong("ts"));
                String lastLoginDt = lastLoginState.value();
                if (lastLoginDt == null || lastLoginDt.equals(curDt)) {
                    //当前为独立用户
                    lastLoginState.update(curDt);
                    out.collect(CartAddUuBean.builder()
                            .cartAddUuCt(1L)
                            .build());
                }
            }
        });
    }

    public static KeyedStream<JSONObject, String> getUserId(SingleOutputStreamOperator<JSONObject> withWaterStream) {
        return withWaterStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("user_id");
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> jsonStream) {
        return jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10L))

                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));
    }

    public static SingleOutputStreamOperator<JSONObject> getJsonStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (ts != null && userId != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);

                    }

                } catch (Exception e) {
                    System.out.println("清洗数据" + s);
                }

            }
        });
    }
}
