package com.puchen.realtime.dws.app;

import com.puchen.base.BaseSQLAPP;
import com.puchen.constant.Constant;
import com.puchen.realtime.dws.function.KwSplit;
import com.puchen.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4, "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //1.读取页面主题数据
        tableEnv.executeSql("" +
                "create table page_info(\n" +
                "       `common` map<STRING,STRING>\n" +
                "       ,`page` map<STRING,STRING>\n" +
                "       ,`ts` bigint\n" +
                "       ,`row_time` as TO_TIMESTAMP_LTZ(ts,3)\n" +
                "       ,WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE, groupId));


        //2.筛选出keywords
        Table keywordsTable = tableEnv.sqlQuery("" +
                "SELECT  page['item'] AS keywords\n" +
                "       ,`row_time`\n" +
                "FROM page_info\n" +
                "WHERE page['last_page_id'] = 'search'\n" +
                "AND page['item_type'] = 'keyword'\n" +
                "AND page['item'] is not null");
        tableEnv.createTemporaryView("keywords_table", keywordsTable);

        //3.创建udtf的分词函数 并且注册
        tableEnv.createTemporarySystemFunction("kwSplit", KwSplit.class);
        //4.调用分词函数对keywords进行拆分
        Table keywordTable = tableEnv.sqlQuery("" +
                "SELECT keywords, keyword ,`row_time`" +
                "FROM keywords_table " +
                "LEFT JOIN LATERAL TABLE(kwSplit(keywords)) ON TRUE");

        tableEnv.createTemporaryView("keyword_table", keywordTable);
        //5.对keyword进行分组开窗聚合

        Table windowAggTable= tableEnv.sqlQuery("" +
                "SELECT  cast(TUMBLE_START(row_time,INTERVAL '10' SECOND) as string) AS stt\n" +
                "       ,cast(TUMBLE_END(row_time,INTERVAL '10' SECOND) as string)   AS edt\n" +
                "       ,cast(CURRENT_DATE as string) as cur_date\n" +
                "       ,keyword\n" +
                "       ,COUNT(*)                                      AS keyword_count\n" +
                "FROM keyword_table\n" +
                "GROUP BY  TUMBLE(row_time,INTERVAL '10' SECOND)\n" +
                "         ,keyword");
        //6.写出到doris  flink需要打开检查点才能导入到doris
        tableEnv.executeSql("" +
                "CREATE TABLE doris_sink (\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING,\n" +
                "    `keyword` STRING,\n" +
                "    keyword_count bigint\n" +
                ")"+SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        windowAggTable.insertInto("doris_sink").execute();
    }
}
