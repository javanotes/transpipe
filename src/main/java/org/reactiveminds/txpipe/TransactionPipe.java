package org.reactiveminds.txpipe;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastJpaDependencyAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

@SpringBootApplication(exclude = {KafkaAutoConfiguration.class,HazelcastAutoConfiguration.class, HazelcastJpaDependencyAutoConfiguration.class})
public class TransactionPipe {

	public static void main(String[] args) {
		SpringApplication.run(TransactionPipe.class, args);
	}

}
