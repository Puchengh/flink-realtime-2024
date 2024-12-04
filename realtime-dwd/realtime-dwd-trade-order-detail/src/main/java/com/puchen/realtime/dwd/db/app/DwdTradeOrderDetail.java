package com.puchen.realtime.dwd.db.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024, 4, "dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //在flink中使用join一定要添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        //核心业务
        //1.读取topic_db
        createTopicDb(groupId, tableEnv);

        //2.筛选订单详情表数据
        filterOd(tableEnv);

        //3.筛选订单信息表
        filterOi(tableEnv);

        //4.筛选详情活动关联表
        filterOda(tableEnv);

        //5.筛选订单详情优惠券关联表

        filterOdc(tableEnv);


        //6.将四张表join合并

        Table joinTable = getJoinTable(tableEnv);
        //7.写出到kafka
        //一旦使用了left join就会产生撤回流 如果需要将数据写出到kafka不能使用一般的kafka 必须使用upsert kafka

        createUpsertKafkaSink(tableEnv);

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();

    }

    private static void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("" +
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(\n" +
                "       id string\n" +
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
                "       ,PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
    }

    private static Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("" +
                "SELECT  od.id\n" +
                "       ,od.order_id\n" +
                "       ,od.sku_id\n" +
                "       ,od.sku_name\n" +
                "       ,od.order_price\n" +
                "       ,od.sku_num\n" +
                "       ,od.create_time\n" +
                "       ,od.split_total_amount\n" +
                "       ,od.split_activity_amount\n" +
                "       ,od.split_coupon_amount\n" +
                "       ,od.operate_time\n" +
                "       ,od.ts\n" +
                "       ,oi.user_id\n" +
                "       ,oi.province_id\n" +
                "       ,oda.activity_id\n" +
                "       ,oda.activity_rule_id\n" +
                "       ,odc.coupon_id\n" +
                "FROM order_detail od\n" +
                "JOIN order_info oi\n" +
                "ON od.order_id = oi.id\n" +
                "LEFT JOIN order_detail_activity oda\n" +
                "ON oda.order_detail_id = od.id\n" +
                "LEFT JOIN order_detail_coupon odc\n" +
                "ON odc.order_detail_id = od.id");
    }

    private static void filterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("" +
                "SELECT  `data`['order_detail_id']  AS order_detail_id\n" +
                "       ,`data`['coupon_id']      AS coupon_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_coupon'\n" +
                "AND `type` = 'insert' ");

        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }

    private static void filterOda(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("SELECT  `data`['order_detail_id']  AS order_detail_id\n" +
                "       ,`data`['activity_id']      AS activity_id\n" +
                "       ,`data`['activity_rule_id'] AS activity_rule_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_activity'\n" +
                "AND `type` = 'insert' ");

        tableEnv.createTemporaryView("order_detail_activity", odaTable);
    }

    private static void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("SELECT  `data`['id']          AS id\n" +
                "       ,`data`['user_id']     AS user_id\n" +
                "       ,`data`['province_id'] AS province_id\n" +
                "       ,ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'insert' ");
        tableEnv.createTemporaryView("order_info", oiTable);
    }

    private static void filterOd(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("SELECT  `data`['id']                    AS id\n" +
                "       ,`data`['order_id']              AS order_id\n" +
                "       ,`data`['sku_id']                AS sku_id\n" +
                "       ,`data`['sku_name']              AS sku_name\n" +
                "       ,`data`['order_price']           AS order_price\n" +
                "       ,`data`['sku_num']               AS sku_num\n" +
                "       ,`data`['create_time']           AS create_time\n" +
                "       ,`data`['split_total_amount']    AS split_total_amount\n" +
                "       ,`data`['split_activity_amount'] AS split_activity_amount\n" +
                "       ,`data`['split_coupon_amount']   AS split_coupon_amount\n" +
                "       ,`data`['operate_time']          AS operate_time\n" +
                "       ,ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail'\n" +
                "AND `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail", odTable);
    }
}
