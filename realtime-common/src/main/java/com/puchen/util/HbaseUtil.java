package com.puchen.util;

import com.alibaba.fastjson.JSONObject;
import com.puchen.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HbaseUtil {

    /**
     * 获取hbase连接
     *
     * @return 一个hbase的同步连接
     */
    public static Connection getConnection() {

        //使用config
        Connection connection = null;
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_ADDREE);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭hbase连接
     *
     * @param connection 一个hbase同步连接
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 创建表格
     *
     * @param connection 一个hbase同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @param families   列族名
     * @throws IOException 获取admin连接异常
     */
    public static void createTable(Connection connection, String namespace, String table, String... families) throws IOException {

        if (families == null || families.length == 0) {
            System.out.println("创建HBase至少有一个列族！");
            return;
        }
        Admin admin = connection.getAdmin();

        //创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                    .build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在,不需要重复创建" + namespace + ":" + table);
        }

        //关闭连接
        admin.close();
    }

    /**
     * 删除表格
     *
     * @param connection 一个hbase同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @throws IOException 获取admin连接异常
     */
    public static void dropTable(Connection connection, String namespace, String table) throws IOException {

        Admin admin = connection.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(namespace, table));
            admin.deleteTable(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        admin.close();
    }

    /**
     * 写入数据到hbase的方法
     *
     * @param connection 同步连接
     * @param namespace
     * @param tableName
     * @param rowKey     主键
     * @param family     列族
     * @param data       jsonobj的key value 对象
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(columnValue));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 获取数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static JSONObject getCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();

        //创建get对象
        try{
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        table.close();
        return jsonObject;
    }

    /**
     * 删除数据
     *
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();

    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
