package org.reactiveminds.txpipe.core.broker;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactiveminds.txpipe.core.broker.PartitionAwareMessageListenerContainer.PartitionListener;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties(KafkaProperties.class)
class KafkaConfiguration {

	private final KafkaProperties properties;
	
	public KafkaConfiguration(KafkaProperties properties) {
		super();
		this.properties = properties;
	}

	@Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          properties.getBootstrapServers());
        configProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          StringSerializer.class);
        configProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
          StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
	@Bean
	AdminClient admin() {
		Map<String, Object> prop = new HashMap<>();
		prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
		prop.put(AdminClientConfig.CLIENT_ID_CONFIG, "producerAdmin");
		prop.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "3000");
		
		return AdminClient.create(prop);
	}
	@Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
	
	@Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          properties.getBootstrapServers());
        props.put(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
          "latest");
        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 
                "false");
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
	/**
	 * Get a consumer listener container
	 * @param topic
	 * @param groupId
	 * @param concurrency
	 * @return
	 */
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	@Bean
    public PartitionAwareMessageListenerContainer 
      kafkaListenerContainer(String topic, String groupId, int concurrency, ErrorHandler errHandler) {
    
		ContainerProperties props = new ContainerProperties(topic);
		props.setAckMode(AckMode.MANUAL_IMMEDIATE);
		props.setGroupId(groupId);
		props.setErrorHandler(errHandler);
		PartitionListener partListener = new PartitionListener(topic);
		props.setConsumerRebalanceListener(partListener);
		PartitionAwareMessageListenerContainer container =  new PartitionAwareMessageListenerContainer(consumerFactory(), props, partListener);
		container.setConcurrency(concurrency);
		return container;
    }
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	KafkaTopicIterator queryTopic(String queryTopic) {
		return new KafkaTopicIterator(queryTopic);
	}
}
