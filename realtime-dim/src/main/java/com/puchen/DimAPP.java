package com.puchen;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.bean.TableProcessDim;
import com.puchen.constant.Constant;
import com.puchen.function.DimBroadcastFunction;
import com.puchen.function.DimHbaseSinkFunction;
import com.puchen.util.FlinkSourceUtil;
import com.puchen.util.HbaseUtil;
import com.puchen.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DimAPP extends BaseAPP {

    public static void main(String[] args) {
        new DimAPP().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //核心业务逻辑 对数据进行处理 核心逻辑 实现dim层维度表的同步任务
        //1.对ods读取的原始数据进行数据清

        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //2.使用flinkCDC读取监控配置表数据

        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE);

        DataStreamSource<String> mysqlSourceStream = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);


//        mysqlSourceStream.print();
        //3.在hbase中创建维度表

        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHbaseTable(mysqlSourceStream).setParallelism(1);

//        createTableStream.print();
        //4.做成广播流
        //广播状态的key  用于判断是否是维度表 value用户补充信息写出到HBase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = createTableStream.broadcast(broadcastState);

        //5.连接主流和广播流

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(jsonObjStream, broadcastStream, broadcastState);

        dimStream.print();

        //6.筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnsStream =  filterColum(dimStream);

        filterColumnsStream.print();
        //7.写出到HBase
        filterColumnsStream.addSink(new DimHbaseSinkFunction());

    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColum(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnsStream = dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {

                JSONObject jsonObject = value.f0;
                TableProcessDim dim = value.f1;
                List<String> columns = Arrays.asList(dim.getSinkColumns().split(","));
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });

        return filterColumnsStream;
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<JSONObject> jsonObjStream, BroadcastStream<TableProcessDim> broadcastStream, MapStateDescriptor<String, TableProcessDim> broadcastState) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = jsonObjStream.connect(broadcastStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectedStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
        return dimStream;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(DataStreamSource<String> mysqlSourceStream) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSourceStream.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {

            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = HbaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭连接
                HbaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                //使用读取到的配置表数据 到hbase中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        // 当配置表发送一个D类型的数据 对应的hbase需要删除一张维度表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    } else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    collector.collect(dim);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim after) {
                String sinkFamily = after.getSinkFamily();
                String[] split = sinkFamily.split(",");

                try {
                    HbaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, after.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim before) {
                try {
                    HbaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, before.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        return createTableStream;
    }


    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        /**
         * 下面这个方法等于filter+map操作，可以等价替换
         */
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) &&
                            !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)
                            && data != null && data.size() != 0) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return jsonObjStream;
    }
}
