/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            .setBootstrapServers("localhost:9092")
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
