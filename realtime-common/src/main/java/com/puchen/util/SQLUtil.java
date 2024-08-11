package com.puchen.util;

import com.puchen.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

    }

    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `ts` BIGINT,\n" +
                "  proc_time as PROCTIME(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3), \n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") " + getKafkaSourceSQL(Constant.TOPIC_DB, groupId)
                ;
    }


    public static String getKafkaSinkSQL(String topicName) {

        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }


    /**
     * 获取upsert kafka的连接  创建表格的语句最后一定要创建主键
     *
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSinkSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";

    }

    /**
     * doris连接器
     * @param tableName
     * @return
     */
    public static String getDorisSinkSQL(String tableName) {

        return "WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '" + Constant.FENONDES + "',\n" +
                "      'table.identifier' = '" + Constant.DORIS_DATEBASE + "." + tableName + "',\n" +
                "      'username' = '" + Constant.DORIS_USERNAME + "',\n" +
                "      'password' = '" + Constant.DORIS_PASSWORD + "',\n" +
                "      'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'\n" +
                ")";
    }
}
