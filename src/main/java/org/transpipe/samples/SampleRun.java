package org.transpipe.samples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastJpaDependencyAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

@SpringBootApplication(exclude = {KafkaAutoConfiguration.class,HazelcastAutoConfiguration.class, HazelcastJpaDependencyAutoConfiguration.class})
public class SampleRun {

	public static void main(String[] args) {
		SpringApplication.run(SampleRun.class, args);
	}

}
