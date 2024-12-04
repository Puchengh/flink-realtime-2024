package com.puchen.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.TableProcessDwd;
import com.puchen.constant.Constant;
import com.puchen.util.FlinkSinkUtil;
import com.puchen.util.FlinkSourceUtil;
import com.puchen.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DwdBaseDb extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心逻辑
        //1.读取topic_db的数据
//        stream.print();

        //2.清洗过滤转化
        SingleOutputStreamOperator<JSONObject> jsonObjStream = filterObjStream(stream);

        //3.读取配置表数据 使用flinksql读取
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE),
                WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);

//        tableProcessDwd.print();

        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDwd tableProcessDwd;

                    if ("op".equals(op)) {
                        tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }

                    tableProcessDwd.setOp(op);
                    collector.collect(tableProcessDwd);

                } catch (Exception e) {
                    System.out.println("获取脏数据" + s);
                }
            }
        }).setParallelism(1);

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> processState = processDwdStream.broadcast(mapStateDescriptor);

        //5.连接主流和广播流 对主流数据进行判断是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(processState)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {


                    HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dwd", TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        //将数据存放到广播状态中
                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String op = value.getOp();
                        String key = value.getSourceTable() + ":" + value.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            hashMap.remove(key);
                        } else {
                            broadcastState.put(key, value);
                        }
                    }

                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        //将广播状态判断当前时候是够需要保留
                        String table = value.getString("table");
                        String type = value.getString("type");
                        String key = table + ":" + type;
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd processDwd = broadcastState.get(key);

                        //二次判断是够是先到的数据
                        if (processDwd == null) {
                            processDwd = hashMap.get(key);
                        }

                        if (processDwd != null) {
                            out.collect(Tuple2.of(value, processDwd));
                        }
                    }


                }).setParallelism(1);
        //筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObject.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));

                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        });
        dataStream.sinkTo(FlinkSinkUtil.getKafakaSinkWithTopicName());
    }

    private static SingleOutputStreamOperator<JSONObject> filterObjStream(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("清洗脏数据" + s);
                }
            }
        });
        return jsonObjStream;
    }
}
