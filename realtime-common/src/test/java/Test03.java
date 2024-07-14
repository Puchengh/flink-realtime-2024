import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Test03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));  //状态的存活时间
//        tableEnv.getConfig().set("table.exec.state.ttl","10");

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  proc_time as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'master:9092',\n" +
                "  'properties.group.id' = 'test03',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
////        Table table = tableEnv.sqlQuery("SELECT  *\n" +
////                "FROM topic_db\n" +
////                "WHERE `database` = 'gmall'\n" +
////                "AND `table` = 'comment_info' ");
////
////        table.execute().print();
//
//        tableEnv.executeSql("SELECT  *\n" +
//                "FROM topic_db\n" +
//                "WHERE `database` = 'gmall'\n" +
//                "AND `table` = 'comment_info' ").print();


        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://slave1:3306/gmall',\n" +
                "   'table-name' = 'base_dic',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'cpCP030216...'\n" +
                ")\n");

//        tableEnv.executeSql("select * from base_dic").print();

        Table common_info = tableEnv.sqlQuery("SELECT  `data`['id']           AS id\n" +
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
//
//        common_info.execute().print();

        tableEnv.createTemporaryView("comment_info",common_info);

        tableEnv.executeSql("SELECT  `id`\n" +
                "       ,`user_id`\n" +
                "       ,`nick_name`\n" +
                "       ,`head_img`\n" +
                "       ,`sku_id`\n" +
                "       ,`spu_id`\n" +
                "       ,`order_id`\n" +
                "       ,`appraise` AS appraise_code\n" +
                "       ,b.dic_name AS appraise_name\n" +
                "       ,`comment_txt`\n" +
                "       ,`create_time`\n" +
                "       ,`operate_time`\n" +
                "FROM comment_info a\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time as b\n" +
                "ON a.appraise = b.dic_code").print();



    }
}
