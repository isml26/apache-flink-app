package FlinkCommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class DataStreamJob {
    public static void main(String[] args) throws Exception {
    /*
           Setup execution environment, which is main entrypoint
           to building Flink apps;
            */
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    String topic = "financial_transactions";

    KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
            .setBootstrapServers("broker:29092")
            .setTopics(topic)
            .setGroupId("flink-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
            .build();

    DataStream<Transaction> transactionDataStream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        transactionDataStream.print();
/*
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
*/
        env.execute("Flink Java API");
    }
}
