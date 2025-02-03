package com.meetme.venkat.kafka_strems_explore.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MappingGKTableConfig {

    @Value("${topic.employee.mapping}")
    private String employeeMappingTopic;

    @Bean(name = "EmployeeRelationTable")
    public GlobalKTable<String, String> customerGlobalKTable(StreamsBuilder builder) {
        return builder.globalTable(
            employeeMappingTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("employee-mapping-store") // Backed by a state store
        );
    }
}