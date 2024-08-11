import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStateBackend(new HashMapStateBackend());  //状态后端

//        // 1.4.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 1.4.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 1.4.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://master:8020/gmall2023/stream/" + "test01");
//        // 1.4.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 1.4.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 1.4.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 1.4.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);



        DataStreamSource<String> kakfaSource = env.fromSource(KafkaSource.<String>builder()
                .setBootstrapServers("master:9092")
                .setTopics("topic_db")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId("test01")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build(), WatermarkStrategy.<String>noWatermarks(), "kakfa_source");

        //对数据源处理
        kakfaSource.print();

        env.execute();
    }
}
