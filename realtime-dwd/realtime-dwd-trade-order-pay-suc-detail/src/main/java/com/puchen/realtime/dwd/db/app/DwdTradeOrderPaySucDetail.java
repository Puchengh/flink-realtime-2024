package com.puchen.realtime.dwd.db.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4,"dwd_trade_order_pay_suc_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心业务逻辑
        //1.读取topic_db主题数据
        createTopicDb(groupId,tableEnv);
        //2.筛选除支付成功的数据
        Table paymentTable = filterPayMentTable(tableEnv);

        tableEnv.createTemporaryView("payment",paymentTable);
        //3.读取下单详情表数据
        createDwdOrderDetail(tableEnv, groupId);
        //4.创建base_dic字典表
        createBaseDic(tableEnv);

//        tableEnv.executeSql("select * from payment").print();


        //5.1使用interval join完成支付成功流和订单详情流数据关联
        Table parOrderTable = intervalJoin(tableEnv);

        tableEnv.createTemporaryView("par_order",parOrderTable);

        //使用lookup join完成维度退化
        Table resultTable = lookUpJoin(tableEnv);

        //7 创建upsert kafka
        createUpsertKafkaSink(tableEnv);

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }

    public static void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("" +
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+" (\n" +
                "        id string\n" +
                "       ,order_id string\n" +
                "       ,user_id string\n" +
                "       ,payment_type_code string\n" +
                "       ,payment_type_name string\n" +
                "       ,payment_time string\n" +
                "       ,sku_id string\n" +
                "       ,sku_name string\n" +
                "       ,order_price string\n" +
                "       ,sku_num string\n" +
                "       ,split_total_amount string\n" +
                "       ,split_activity_amount string\n" +
                "       ,split_coupon_amount string\n" +
                "       ,operate_time string\n" +
                "       ,province_id string\n" +
                "       ,activity_id string\n" +
                "       ,activity_rule_id string\n" +
                "       ,coupon_id string\n" +
                "       ,ts bigint\n" +
                "       ,PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS)
        );
    }

    public static Table lookUpJoin(StreamTableEnvironment tableEnv) {
        Table resultTable = tableEnv.sqlQuery("" +
                "SELECT  id\n" +
                "       ,order_id\n" +
                "       ,user_id\n" +
                "       ,payment_type as payment_type_code\n" +
                "       ,info.dic_name as payment_type_name\n" +
                "       ,payment_time\n" +
                "       ,sku_id\n" +
                "       ,sku_name\n" +
                "       ,order_price\n" +
                "       ,sku_num\n" +
                "       ,split_total_amount\n" +
                "       ,split_activity_amount\n" +
                "       ,split_coupon_amount\n" +
                "       ,operate_time\n" +
                "       ,province_id\n" +
                "       ,activity_id\n" +
                "       ,activity_rule_id\n" +
                "       ,coupon_id\n" +
                "       ,ts\n" +
                "FROM par_order p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b\n" +
                "on p.payment_type = b.rowkey");
        return resultTable;
    }

    public static Table intervalJoin(StreamTableEnvironment tableEnv) {
        Table parOrderTable = tableEnv.sqlQuery("" +
                "SELECT  od.id\n" +
                "       ,p.order_id\n" +
                "       ,p.user_id\n" +
                "       ,payment_type\n" +
                "       ,callback_time AS payment_time\n" +
                "       ,sku_id\n" +
                "       ,sku_name\n" +
                "       ,order_price\n" +
                "       ,sku_num\n" +
                "       ,split_total_amount\n" +
                "       ,split_activity_amount\n" +
                "       ,split_coupon_amount\n" +
                "       ,operate_time\n" +
                "       ,province_id\n" +
                "       ,activity_id\n" +
                "       ,activity_rule_id\n" +
                "       ,coupon_id\n" +
                "       ,p.ts\n" +
                "       ,p.proc_time\n" +
                "FROM payment p, order_detail od\n" +
                "WHERE p.order_id = od.order_id\n" +
                "AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND ");
        return parOrderTable;
    }

    public static void createDwdOrderDetail(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("" +
                "create table order_detail (\n" +
                "        id string\n" +
                "       ,order_id string\n" +
                "       ,sku_id string\n" +
                "       ,sku_name string\n" +
                "       ,order_price string\n" +
                "       ,sku_num string\n" +
                "       ,create_time string\n" +
                "       ,split_total_amount string\n" +
                "       ,split_activity_amount string\n" +
                "       ,split_coupon_amount string\n" +
                "       ,operate_time string\n" +
                "       ,ts bigint\n" +
                "       ,user_id string\n" +
                "       ,province_id string\n" +
                "       ,activity_id string\n" +
                "       ,activity_rule_id string\n" +
                "       ,coupon_id string" +
                "       ,row_time as TO_TIMESTAMP_LTZ(ts * 1000,3)" +
                "       ,WATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }

    public static Table filterPayMentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("" +
                "SELECT  `data`['id']            AS id\n" +
                "       ,`data`['order_id']      AS order_id\n" +
                "       ,`data`['user_id']       AS user_id\n" +
                "       ,`data`['payment_type']  AS payment_type\n" +
                "       ,`data`['total_amount']  AS total_amount\n" +
                "       ,`data`['callback_time'] AS callback_time\n" +
                "       ,ts\n" +
                "       ,row_time\n" +
                "       ,proc_time\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'payment_info'\n" +
                "AND `type` = 'update'\n" +
                "AND `old`['payment_status'] is not null\n" +
                "AND `data`['payment_status'] = '1602'");
        return paymentTable;
    }
}
