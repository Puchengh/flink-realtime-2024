package com.puchen.function;

import com.alibaba.fastjson.JSONObject;
import com.puchen.bean.TableProcessDim;
import com.puchen.constant.Constant;
import com.puchen.util.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

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
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, SinkFunction.Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim dim = value.f1;
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");
        //分四种  insert update delete bootstrap-insert
        if("delete".equals(type)){
            //删除对应的维度表数据
            deleteHbase(data,dim);
        }else {
            //覆盖写入的维度表数据
            putHbase(data,dim);

        }
    }

    private void putHbase(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRokeyValue = data.getString(dim.getSinkRowKey());
        String sinkFamily = dim.getSinkFamily();
        try {
            HbaseUtil.putCells(connection, Constant.HBASE_NAMESPACE,sinkTable,sinkRokeyValue,sinkFamily,data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteHbase(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRokeyValue = data.getString(dim.getSinkRowKey());
        try {
            HbaseUtil.deleteCells(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkRokeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
