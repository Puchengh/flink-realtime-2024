package com.puchen.util;

import com.alibaba.fastjson.JSONObject;
import com.puchen.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
}
