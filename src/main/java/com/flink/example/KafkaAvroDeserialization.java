package com.flink.example;

import java.io.IOException;
import java.util.stream.Collector;

import org.acme.kafka.quarkus.Observation;
import org.acme.kafka.quarkus.RedpandaKey;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class KafkaAvroDeserialization 
        implements KafkaRecordDeserializationSchema<Tuple2<RedpandaKey,Observation>> {

    private static final long serialVersionUID = 1L;
    

    //TODO: accept 
    DeserializationSchema deserialzationSchemaVal = ConfluentRegistryAvroDeserializationSchema.forSpecific(Observation.class, "http://localhost:8081");
    DeserializationSchema deserialzationSchemaKey = ConfluentRegistryAvroDeserializationSchema.forSpecific(RedpandaKey.class, "http://localhost:8081");

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
    }

    @Override
    public TypeInformation<Tuple2<RedpandaKey,Observation>> getProducedType() {
        TypeInformation<Tuple2<RedpandaKey,Observation>> typeInformation =TypeInformation.of(new TypeHint<Tuple2<RedpandaKey,Observation>>() {  	}); 
        return typeInformation;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
            org.apache.flink.util.Collector<Tuple2<RedpandaKey, Observation>> out) throws IOException {
        // TODO Auto-generated method stub
        Observation obs = (Observation)deserialzationSchemaVal.deserialize(record.value());
        RedpandaKey redpandaKey =(RedpandaKey) deserialzationSchemaKey.deserialize(record.key());
        out.collect(Tuple2.of(redpandaKey, obs));
    }
}
