package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.UserLoginBean;
import com.puchen.constant.Constant;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.读取页面数据
//        stream.print();

        //2.对数据进行清洗罗过滤 -> UID不为空
        SingleOutputStreamOperator<JSONObject> jsonStream = getJsonStream(stream);
        //3.注册水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = getTs(jsonStream);
        //4.按照uid分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(withWaterMarkStream);
        //5.判断独立用户和回流用户
        SingleOutputStreamOperator<UserLoginBean> processStream = getProcessStream(keyedStream);
//        processStream.print();
        //6.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduceStream = getReduce(processStream);
//        reduceStream.print();
        //7.写入doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));

    }

    public static KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
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
                }));
    }

    public static SingleOutputStreamOperator<JSONObject> getJsonStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String uid = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    Long ts = jsonObject.getLong("ts");
                    if (uid != null && ts != null && (lastPageId == null || "login".equals(lastPageId))) {
                        //当前为一次会话的第一条登录数据
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("清洗脏数据" + s);
                }
            }
        });
    }

    public static SingleOutputStreamOperator<UserLoginBean> getReduce(SingleOutputStreamOperator<UserLoginBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserLoginBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }

                    }
                });
    }

    public static SingleOutputStreamOperator<UserLoginBean> getProcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

            ValueState<String> lastLoginDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);

                lastLoginDt = getRuntimeContext().getState(lastLoginDtDesc);

            }


            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                //比较当前登录的日期和状态存储的日期
                String lastLoginDate = lastLoginDt.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;
//                if (lastLoginDate == null) {
//                    //新的访客数据
//                    uuCt = 1L;
//                    lastLoginDt.update(curDt);
//                } else if (ts - DateFormatUtil.dateToTs(lastLoginDate) > 7 * 24 * 60 * 60 * 1000L) {
//                    //当前是回流用户
//                    backCt = 1L;
//                    uuCt = 1L;
//                    lastLoginDt.update(curDt);
//                } else if (!lastLoginDate.equals(curDt)) {
//                    //之前有登录 但不是今天
//                    uuCt = 1L;
//                    lastLoginDt.update(curDt);
//                }else {
//                    //状态不为空 今天的有一次登录
//                }

                //判断独立用户
                if(lastLoginDate == null || !lastLoginDate.equals(curDt)){
                    uuCt = 1L;
                }
                //判断回流用户
                if (lastLoginDate != null && ts - DateFormatUtil.dateToTs(lastLoginDate) > 7 * 24 * 60 * 60 * 1000L) {
                    //当前是回流用户
                    backCt = 1L;
                }
                lastLoginDt.update(curDt);

                //不是独立用户肯定不是回流用户 不需要下游统计
                if(uuCt != 0L){
                    out.collect(new UserLoginBean("","","",backCt,uuCt,ts));
                }
            }
        });
    }
}
