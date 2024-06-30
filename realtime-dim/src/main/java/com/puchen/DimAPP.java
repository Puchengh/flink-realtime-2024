package com.puchen;

import com.alibaba.fastjson.JSONObject;
import com.puchen.base.BaseAPP;
import com.puchen.constant.Constant;
import com.puchen.util.FlinkSourceUtil;
import com.puchen.util.HbaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

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

        mysqlSourceStream.flatMap(new RichFlatMapFunction<String, JSONObject>() {

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
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                //使用读取到的配置表数据 到hbase中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        //4.做成广播流

        //5.连接主流和广播流

        //6.筛选出需要写出的字段

        //7.写出到HBase
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
                    JSONObject data = jsonObject.getJSONObject("date");
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
