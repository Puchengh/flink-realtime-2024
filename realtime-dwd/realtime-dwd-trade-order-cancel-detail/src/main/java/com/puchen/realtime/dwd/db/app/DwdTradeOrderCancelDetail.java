package com.puchen.realtime.dwd.db.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwdTradeOrderCancelDetail extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                10015,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_CANCEL
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 1. 读取 topic_db 数据
        createTopicDb(groupId, tableEnv);
        // 2. 读取 dwd 层下单事务事实表数据
        Table orderinfo = tableEnv.sqlQuery(
                "SELECT  `data`['id']                    AS id\n" +
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
                        "AND `table` = 'order_detail'");
        tableEnv.createTemporaryView("order_info",orderinfo);

        // 3. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 4. 订单取消表和下单表进行 join
        Table result = tableEnv.sqlQuery(
                "select  od.id,\n" +
                        "od.order_id,\n" +
                        "od.sku_id,\n" +
                        "od.sku_name,\n" +
                        "od.order_price,\n" +
                        "od.sku_num,\n" +
                        "od.create_time,\n" +
                        "od.split_total_amount,\n" +
                        "od.split_activity_amount,\n" +
                        "od.split_coupon_amount,\n" +
                        "od.operate_time as odoperate_time," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "oc.ts " +
                        "from order_info od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // 5. 写出
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                        "id string\n" +
                        ",order_id string\n" +
                        ",sku_id string\n" +
                        ",sku_name string\n" +
                        ",order_price string\n" +
                        ",sku_num string\n" +
                        ",create_time string\n" +
                        ",split_total_amount string\n" +
                        ",split_activity_amount string\n" +
                        ",split_coupon_amount string\n" +
                        ",odoperate_time string\n" +
                        ",order_cancel_date_id string\n" +
                        ",operate_time string\n" +
                        ",ts bigint" +
                        ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
