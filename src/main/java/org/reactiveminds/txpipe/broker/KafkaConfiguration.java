package org.reactiveminds.txpipe.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactiveminds.txpipe.broker.PartitionAwareMessageListenerContainer.PartitionListener;
import org.reactiveminds.txpipe.err.BrokerException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
class KafkaConfiguration  {

	private final KafkaProperties properties;
	@Value("${txpipe.core.instanceId}")
	private String groupId;
	
	public KafkaConfiguration(KafkaProperties properties) {
		super();
		this.properties = properties;
	}
	
	static class ConsumerOffsetWrapper{
		private Map<String, Set<String>> unwrapped = new HashMap<>();
		public void add(ListConsumerGroupOffsetsResult groupOff, String group) {
			try 
			{
				Set<String> topics = groupOff.partitionsToOffsetAndMetadata().get()
				.keySet().stream().map(t -> t.topic()).collect(Collectors.toSet());
				
				topics.forEach(t -> {
					if(!getUnwrapped().containsKey(t))
						getUnwrapped().put(t, new HashSet<>());
					
					getUnwrapped().get(t).add(group);
				});
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				throw new BrokerException("While invoking partitionsToOffsetAndMetadata", e);
			}
		}
		public Map<String, Set<String>> getUnwrapped() {
			return unwrapped;
		}
		public void setUnwrapped(Map<String, Set<String>> unwrapped) {
			this.unwrapped = unwrapped;
		}
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
	/**
	 * 
	 */
	static final String TXPIPE_REPLY_QUEUE = "txnReply";
	@Bean
    NewTopic topic() {
        return new NewTopic(TXPIPE_REPLY_QUEUE, 8, (short) 1);
    }
	@Bean
    public RequestReplyKafkaTemplate txnRequestReplyTemplate() {
		ContainerProperties containerProperties = new ContainerProperties(TXPIPE_REPLY_QUEUE);
		containerProperties.setGroupId(groupId);
		KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);
        return new RequestReplyKafkaTemplate(producerFactory(), container);
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
	 * @param primaryTopic
	 * @param groupId
	 * @param concurrency
	 * @return
	 */
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	@Bean
    public PartitionAwareMessageListenerContainer 
      kafkaListenerContainer(String primaryTopic, String groupId, int concurrency, ErrorHandler errHandler) {
		ContainerProperties props = new ContainerProperties(primaryTopic);
		props.setAckMode(AckMode.MANUAL_IMMEDIATE);
		props.setGroupId(groupId);
		props.setErrorHandler(errHandler);
		PartitionListener partListener = new PartitionListener(primaryTopic);
		props.setConsumerRebalanceListener(partListener);
		PartitionAwareMessageListenerContainer container =  new PartitionAwareMessageListenerContainer(consumerFactory(), props, partListener);
		container.setConcurrency(concurrency);
		container.setBeanName(primaryTopic);
		return container;
    }
	
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	KafkaTopicIterator topicIterator(String queryTopic) {
		return new KafkaTopicIterator(queryTopic);
	}
	@Bean
	KafkaAdminSupport adminSupport() {
		return new KafkaAdminSupportImpl();
	}
}
