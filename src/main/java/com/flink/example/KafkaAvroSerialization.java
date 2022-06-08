package com.flink.example;

import org.acme.kafka.quarkus.Observation;
import org.acme.kafka.quarkus.RedpandaKey;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaAvroSerialization implements KafkaRecordSerializationSchema<Tuple2<RedpandaKey,Observation>> {

    SerializationSchema serialzationSchemaVal = ConfluentRegistryAvroSerializationSchema.forSpecific(Observation.class,"obs-key", "http://localhost:8081");
    SerializationSchema serialzationSchemaKey = ConfluentRegistryAvroSerializationSchema.forSpecific(RedpandaKey.class,"obs-val","http://localhost:8081");

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<RedpandaKey, Observation> element, KafkaSinkContext context,
            Long timestamp) {
        // TODO Auto-generated method stub
        ProducerRecord<byte[],byte[]> producerRecord =new ProducerRecord<byte[],byte[]>("flink-poc-2", serialzationSchemaKey.serialize(element.f0), serialzationSchemaVal.serialize(element.f1)) ;        
        return producerRecord;
    }
    
}
