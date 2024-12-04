package com.puchen.realtime.dwd.db.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, "dwd_trade_cart_add");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        createTopicDb(groupId, tableEnv);
        Table cartAddTable = filterCartAdd(tableEnv);

        createKafkaSink(tableEnv);

        //写出晒选的数据到对应的主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();

    }

    public static void createKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                        "id string\n" +
                        ",user_id string\n" +
                        ",sku_id string\n" +
                        ",cart_price string\n" +
                        ",sku_num string\n" +
                        ",sku_name string\n" +
                        ",is_checked string\n" +
                        ",create_time string\n" +
                        ",operate_time string\n" +
                        ",is_ordered string\n" +
                        ",order_time string\n" +
                        ",ts bigint\n" +
                        ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
    }

    public static Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT  \n" +
                "`data`['id'] as id\n" +
                ",`data`['user_id'] as user_id\n" +
                ",`data`['sku_id'] as sku_id\n" +
                ",`data`['cart_price'] as cart_price\n" +
                ",if(`type` = 'insert',`data`['sku_num'],cast( cast(`data`['sku_num'] AS bigint) - cast(`old`['sku_num'] AS bigint) as string)) as sku_num\n" +
                ",`data`['sku_name'] as sku_name\n" +
                ",`data`['is_checked'] as is_checked\n" +
                ",`data`['create_time'] as create_time\n" +
                ",`data`['operate_time'] as operate_time\n" +
                ",`data`['is_ordered'] as is_ordered\n" +
                ",`data`['order_time'] as order_time\n" +
                ",ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'cart_info'\n" +
                "AND (`type` = 'insert' \n" +
                "    or (`type` = 'update' \n" +
                "        AND `old`['sku_num'] is not null \n" +
                "        AND cast(`data`['sku_num'] AS bigint) > cast(`old`['sku_num'] AS bigint)))");
    }
}
