package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.UserRegisterBean;
import com.puchen.constant.Constant;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                10025,
                4,
                "dws_user_user_register_window",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.读取dwd主题的表的数据
//        stream.print();
        //2.数据清洗过滤
        //3.转化数据结构为javabean
        SingleOutputStreamOperator<UserRegisterBean> beanStream = getBeanStream(stream);
        //4.添加水位线
        SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream = getWithWaterMarkStream(beanStream);
        //5.分组开窗聚合

        SingleOutputStreamOperator<UserRegisterBean> reduceStream = getReduce(withWaterMarkStream);
//        reduceStream.print();
        //6.写入doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));
    }

    public static SingleOutputStreamOperator<UserRegisterBean> getReduce(SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream) {
        return withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = DateFormatUtil.tsToDateTime(start);
                        String edt = DateFormatUtil.tsToDateTime(end);
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                        for (UserRegisterBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }
                    }
                });
    }

    public static SingleOutputStreamOperator<UserRegisterBean> getWithWaterMarkStream(SingleOutputStreamOperator<UserRegisterBean> beanStream) {
        return beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return DateFormatUtil.dateTimeToTs(userRegisterBean.getCreateTime());
                    }
                })
        );
    }

    public static SingleOutputStreamOperator<UserRegisterBean> getBeanStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String value, Collector<UserRegisterBean> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String createTime = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (createTime != null && id != null) {
                        out.collect(UserRegisterBean.builder()
                                .registerCt(1L)
                                .createTime(createTime)
                                .build());
                    }
                } catch (Exception e) {
                    System.out.println("过滤脏数据" + value);
                }
            }
        });
    }
}
