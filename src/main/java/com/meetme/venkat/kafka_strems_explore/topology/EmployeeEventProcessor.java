package com.meetme.venkat.kafka_strems_explore.topology;

import com.meetme.venkat.kafka_strems_explore.model.Employee;
import com.meetme.venkat.kafka_strems_explore.utils.GenericSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EmployeeEventProcessor {

    @Value("${topic.employee.request}")
    private String employeeInputTopic;

    @Value("${topic.employee.response}")
    private String employeeOutputTopic;

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private GlobalKTable<String, String> EmployeeRelationTable;

    @Autowired
    void streamMessages(StreamsBuilder streamsBuilder) {
        Serde<Employee> employeeSerde = GenericSerde.getSerde(Employee.class);
        KStream<String, Employee> kStream = streamsBuilder.stream(employeeInputTopic, Consumed.with(STRING_SERDE, employeeSerde));
        kStream.leftJoin(
                EmployeeRelationTable,
                (key, value) -> key,
                (streamValue, gTableValue) -> {
                    if (gTableValue != null) {
                        streamValue.getPersonalInformation().setSsn(gTableValue);
                        return new KeyValue<>(gTableValue, streamValue);
                    } else {
                        return new KeyValue<>("not-found", streamValue);
                    }
                })
                .map((oldKey, keyValuePair) -> new KeyValue<>(keyValuePair.key, keyValuePair.value))
                .peek((k,v) -> log.info("recored processed Key: " +k +"with value: " +v))
                .split()
                .branch((key, value) -> key.equals("not-found"), Branched.withConsumer(ks -> ks
                        .peek((k, v) -> log.info("Matching Employee SSN not found in GKTable"))
                ))
                .branch((key, value) -> !key.equals("not-found"), Branched.withConsumer(ks -> { ks
                        .peek((k, v) -> log.info("Matching Employee SSN found in GKTable"))
                        .to(employeeOutputTopic, Produced.with(STRING_SERDE, employeeSerde));
                }));

    }
}
