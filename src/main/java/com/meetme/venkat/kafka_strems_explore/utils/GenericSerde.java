package com.meetme.venkat.kafka_strems_explore.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.IOException;

public class GenericSerde {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
    }

    public static <T> Serde<T> getSerde(Class<T> clazz) {
        JsonSerializer<T> jsonSerializer = new JsonSerializer<>(objectMapper);
        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(clazz, objectMapper, false);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
