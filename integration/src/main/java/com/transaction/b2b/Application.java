package com.transaction.b2b;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

/**
 * It is to be noted that {@linkplain KafkaAutoConfiguration} needs to be excluded, either
 * using annotations as below, or via configuration property 'spring.autoconfigure.exclude'
 * @author Sutanu_Dalui
 *
 */
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
