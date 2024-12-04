package com.puchen.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.puchen.base.BaseAPP;
import com.puchen.bean.TrafficPageViewBean;
import com.puchen.constant.Constant;
import com.puchen.function.DorisMapFunction;
import com.puchen.util.DateFormatUtil;
import com.puchen.util.FlinkSinkUtil;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Concat;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {


    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.读取dwd的page主题数据
//        stream.print();

        //2.清洗过滤 转换结构为jsonOBJ
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    Long ts = jsonObject.getLong("ts");
                    String mid = jsonObject.getJSONObject("common").getString("mid");

                    if (mid != null && ts != null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤脏数据" + value);
                }
            }
        });

        //3.按照mid进行分组 判断独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                //设置状态的存活时间  Time.days(1L) 24小时的意思
                lastLoginDtDesc.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  //默认是OnCreateAndWrite
                        .build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                //判断独立访客
                Long ts = jsonObject.getLong("ts");
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String curDt = DateFormatUtil.tsToDate(ts);

                String lastLongDt = lastLoginDtState.value();
                Long uvCt = 0L;
                Long svCt = 0L;
                if (lastLongDt == null || !lastLongDt.equals(curDt)) {
                    //状态没有存日期 或者状态的日期不是同一天 当前是一条独立访客
                    uvCt = 1L;
                }

                //判断会话数的方法
                //判断last_page_id == null 新会话的开始
                String lastPageId = page.getString("last_page_id");
                if (lastPageId == null) {
                    //新的会话
                    svCt = 1L;

                    //如果是独立访客 更新一下状态
                    lastLoginDtState.update(curDt);
                }
                collector.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .sid(common.getString("sid"))
                        .ts(ts)
                        .durSum(page.getLong("during_time"))
                        .build());
            }
        });

//        beanStream.print();
        //4.添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                })
        );
        //5.按照粒度分组
        KeyedStream<TrafficPageViewBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {
                return value.getVc() + ":" + value.getCh() + ":" + value.getAr() + ":" + value.getIsNew();
            }
        });

        //6.开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        //7.集合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceFullStream = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                //将多个元素的累加值累计到一起

                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {

                TimeWindow window = context.window();
                String start = DateFormatUtil.tsToDate(window.getStart());
                String end = DateFormatUtil.tsToDate(window.getEnd());
                String partition = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                for (TrafficPageViewBean element : iterable) {
                    element.setStt(start);
                    element.setEdt(end);
                    element.setCur_date(partition);
                    collector.collect(element);
                }
            }
        });

//        reduceFullStream.print();

        //8.写出到doris
        reduceFullStream.map(new DorisMapFunction<TrafficPageViewBean>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));

    }
}
