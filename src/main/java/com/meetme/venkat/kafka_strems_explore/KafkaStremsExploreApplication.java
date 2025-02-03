package com.meetme.venkat.kafka_strems_explore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStremsExploreApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStremsExploreApplication.class, args);
	}

}
