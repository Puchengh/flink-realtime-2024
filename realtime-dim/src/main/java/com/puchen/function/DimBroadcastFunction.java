package com.puchen.function;
import com.alibaba.fastjson.JSONObject;
import com.puchen.bean.TableProcessDim;
import com.puchen.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    public HashMap<String, TableProcessDim> hashMap;
    MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度信息
//                java.sql.Connection connection = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dim",
                TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);

    }

    /**
     * 处理主流数据
     * @param jsonObject
     * @param readOnlyContext
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim,
            Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //读取广播状态

        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);

        //查询广播状态 判断当前的数据对应的表格是否存在于状态里面
        String tableName = jsonObject.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);
//        System.out.println(tableProcessDim);

        //如果是数据到的太早造成状态为空
        if (tableProcessDim == null) {
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            //状态不为空 说明当前行数据是维度表数据
            collector.collect(Tuple2.of(jsonObject, tableProcessDim));
        }
    }

    /**
     * 处理广播流数据
     * @param tableProcessDim
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {


        //读取广播状态
        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);

        //将配置表信息作为一个维度表的标记 写到广播状态
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(tableProcessDim.getSourceTable());
            //同步删除hashmap中初始化加载的配置表信息
            hashMap.remove(tableProcessDim.getSourceTable());
        }else {
            tableProcessState.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
    }

}
