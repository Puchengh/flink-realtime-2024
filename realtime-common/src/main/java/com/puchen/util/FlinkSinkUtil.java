package com.puchen.util;

import com.alibaba.fastjson.JSONObject;
import com.puchen.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("puchen-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") //15分钟
                .build();

    }

    public static KafkaSink<JSONObject> getKafakaSinkWithTopicName() {
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                                         @Nullable
                                         @Override
                                         public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                             String topicName = jsonObject.getString("sink_table");
                                             jsonObject.remove("sink_table");
                                             return new ProducerRecord<byte[], byte[]>(topicName, Bytes.toBytes(jsonObject.toJSONString()));
                                         }
                                     }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("puchen-" + "base_db" + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") //15分钟
                .build();

    }


    public static DorisSink<String> getDorisSink(String tableName) {

        Properties properties = new Properties();
        // 上游是 json 写入时，需要开启配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setLabelPrefix("label-doris" + System.currentTimeMillis()) //streamload label prefix
                                .setDeletable(false)
                                .setStreamLoadProp(properties).build()
                )
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.FENONDES)
                        .setTableIdentifier(Constant.DORIS_DATEBASE + "." + tableName)
                        .setUsername(Constant.DORIS_USERNAME)
                        .setPassword(Constant.DORIS_PASSWORD)
                        .build())
                .build();
    }
}
