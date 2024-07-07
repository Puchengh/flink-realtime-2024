package com.puchen.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.constant.Constant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class DwdBaseLog extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //etl过滤不完整的数据
        stream.print();

        //进行新旧访客修复
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etl(stream);
        jsonObjectStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (mid != null && ts != null)
                            out.collect(jsonObject);
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("过滤脏数据" + s);
                }

            }
        });
    }
}
