package com.flink.example;

import org.acme.kafka.quarkus.Observation;
import org.acme.kafka.quarkus.RedpandaKey;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/** Hello world! */
public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Flink Job");
        // ConfluentRegistryAvroDeserializationSchema.forSpecific(
        //                     Observation.class, "http://localhost:8081"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaSource<Tuple2<RedpandaKey,Observation>> source =
                KafkaSource.<Tuple2<RedpandaKey,Observation>>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("sample_avro_topic")
                        .setGroupId("my-group-2")
                        .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
                        .setDeserializer(new KafkaAvroDeserialization()) 
                        .build();

                        KafkaSink<Tuple2<RedpandaKey,Observation>> sink =
                KafkaSink.<Tuple2<RedpandaKey,Observation>>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(new KafkaAvroSerialization())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(
                        new MapFunction<Tuple2<RedpandaKey,Observation>, Tuple2<RedpandaKey,Observation>>() {

                            @Override
                            public Tuple2<RedpandaKey,Observation> map(Tuple2<RedpandaKey,Observation> s) throws Exception {
                               
                                System.out.println(s.f0.getValue());
                                return s;
                            }
                        })
                .sinkTo(sink);
                
        env.execute();
    }
}
