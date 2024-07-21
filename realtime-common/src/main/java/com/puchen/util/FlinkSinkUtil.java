package com.puchen.util;

import com.alibaba.fastjson.JSONObject;
import com.puchen.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

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

    public static KafkaSink<JSONObject> getKafakaSinkWithTopicName(){
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
}
