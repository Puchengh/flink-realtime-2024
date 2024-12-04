package com.puchen.realtime.dwd.db.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd_interaction_comment_info");
    }


    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //读取topic_db
        createTopicDb(groupId, tableEnv);

        //读取base_dic
        createBaseDic(tableEnv);

        //侵袭topic_db 筛选出评论信息表新增的数据
        filterCommentInfo(tableEnv);

        //使用lookup 关联维度信息
        Table joinTable = lookupJoin(tableEnv);

        //创建kafaka对应的表格
        createKakfaSink(tableEnv);

        //写出到对应的kafka主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();

    }

    private static void createKakfaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
                "id STRING,\n" +
                "user_id STRING,\n" +
                "nick_name STRING,\n" +
                "head_img STRING,\n" +
                "sku_id STRING,\n" +
                "spu_id STRING,\n" +
                "order_id STRING,\n" +
                "appraise_code STRING,\n" +
                "appraise_name STRING,\n" +
                "comment_txt STRING,\n" +
                "create_time STRING,\n" +
                "operate_time STRING" +
                ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    private static Table lookupJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT  `id`\n" +
                "       ,`user_id`\n" +
                "       ,`nick_name`\n" +
                "       ,`head_img`\n" +
                "       ,`sku_id`\n" +
                "       ,`spu_id`\n" +
                "       ,`order_id`\n" +
                "       ,`appraise` AS appraise_code\n" +
                "       ,info.dic_name AS appraise_name\n" +
                "       ,`comment_txt`\n" +
                "       ,`create_time`\n" +
                "       ,`operate_time`\n" +
                "FROM comment_info a\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time as b\n" +
                "ON a.appraise = b.rowkey");
    }

    private static void filterCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfo = tableEnv.sqlQuery("SELECT  `data`['id']           AS id\n" +
                "       ,`data`['user_id']      AS user_id\n" +
                "       ,`data`['nick_name']    AS nick_name\n" +
                "       ,`data`['head_img']     AS head_img\n" +
                "       ,`data`['sku_id']       AS sku_id\n" +
                "       ,`data`['spu_id']       AS spu_id\n" +
                "       ,`data`['order_id']     AS order_id\n" +
                "       ,`data`['appraise']     AS appraise\n" +
                "       ,`data`['comment_txt']  AS comment_txt\n" +
                "       ,`data`['create_time']  AS create_time\n" +
                "       ,`data`['operate_time'] AS operate_time\n" +
                "       ,proc_time\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'comment_info'\n" +
                "AND `type` = 'insert'" +
                "");
        tableEnv.createTemporaryView("comment_info", commentInfo);
    }
}
