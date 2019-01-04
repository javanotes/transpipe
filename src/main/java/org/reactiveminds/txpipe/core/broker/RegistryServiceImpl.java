package org.reactiveminds.txpipe.core.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.ComponentDef;
import org.reactiveminds.txpipe.core.ComponentManager;
import org.reactiveminds.txpipe.core.JsonMapper;
import org.reactiveminds.txpipe.core.PipelineDef;
import org.reactiveminds.txpipe.core.RegistryService;
import org.reactiveminds.txpipe.core.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.StringUtils;

class RegistryServiceImpl implements RegistryService,AcknowledgingConsumerAwareMessageListener<String,String> {

	private static final Logger log = LoggerFactory.getLogger(RegistryServiceImpl.class);
	@Autowired
	BeanFactory beans;
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	ConsumerFactory<String, String> consumerFactory;
	@Autowired
	BeanFactory beanFactory;
	@Autowired
	KafkaPublisher pubAdmin;
	
	private JsonMapper mapper = new JsonMapper();
	@Value("${txpipe.broker.orchestrationTopic:managerTopic}")
	private String orchestrationTopic;
	
	private ConcurrentMap<String, PipelineDef> register = new ConcurrentHashMap<>();
	private ConcurrentMessageListenerContainer<String, String> container;
	
	@Value("${txpipe.instanceId}")
	private String groupId;
	
	@SuppressWarnings("unchecked")
	@PostConstruct
	private void onStart() {
		container = (ConcurrentMessageListenerContainer<String, String>) beans.getBean("kafkaListenerContainer", orchestrationTopic, groupId, 1, new KafkaConsumer.ContainerErrHandler());
		container.setupMessageListener(this);
		container.start();
		
		AllDefinitionLoader loader = new AllDefinitionLoader();
		loader.run();
		loader.allDefinitions.forEach(def -> put(def));
		log.info("Loaded all existing definitions into registry ..");
	}
	@PreDestroy
	private void destroy() {
		container.stop();
	}
	private class AllDefinitionLoader implements Runnable{

		private Map<TopicPartition, Long> endOffsets;
		private Consumer<String, String> consumer;
		private final List<PipelineDef> allDefinitions = new ArrayList<>();
		@Override
		public void run() {
			
			try {
				consumer = consumerFactory.createConsumer(UUID.randomUUID().toString(), "");
				List<TopicPartition> topicParts = consumer.partitionsFor(orchestrationTopic).stream()
						.map(p -> new TopicPartition(orchestrationTopic, p.partition())).collect(Collectors.toList());

				consumer.assign(topicParts);
				consumer.seekToBeginning(consumer.assignment());
				endOffsets = consumer.endOffsets(consumer.assignment());
				
				//the data has to come in order. else we cannot convert stream to table. that is why we should keep the partition as 1.
				while (hasPendingMessages()) {
					ConsumerRecords<String, String> records = consumer.poll(10);
					records.forEach(c -> allDefinitions.add(mapper.toObject(c.value(), PipelineDef.class)));
					consumer.commitSync();
				}
				
			} 
			catch (Exception e) {
				log.error("Loading of existing definitions failed on startup!", e);
			}
			finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		
		private boolean hasPendingMessages() {
			return endOffsets.entrySet().stream().anyMatch(e -> e.getValue() > consumer.position(e.getKey()));
		}
		
	}
	@Override
	public void add(PipelineDef pipeline) {
		broadcast(pipeline);
	}

	private void put(PipelineDef def) {
		def.getComponents().forEach(c -> startComponent(c));
		register.put(def.getPipelineId(), def);
		log.info("Pipeline definition registered - " + def.getPipelineId()+". (Not all services may be running, however)");
		log.debug(def.toString());
	}
	@Override
	public boolean contains(String pipelineId) {
		return register.containsKey(pipelineId);
	}

	@Override
	public PipelineDef get(String pipelineId) {
		return register.get(pipelineId);
	}

	@Override
	public String getSource(String pipelineId) {
		if(contains(pipelineId)) {
			return get(pipelineId).getOpeningChannel();
		}
		return null;
	}

	private void broadcast(PipelineDef pipeline) {
		kafkaTemplate.send(orchestrationTopic, mapper.toJson(pipeline));
	}

	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		PipelineDef def = mapper.toObject(data.value(), PipelineDef.class);
		put(def);
		acknowledgment.acknowledge();
	}
	
	
	//private ConcurrentMap<String, Boolean> startedComponents = new ConcurrentHashMap<>();
	
	@Value("${txpipe.broker.topicPartition:10}")
	private int partition;
	@Value("${txpipe.broker.topicReplica:1}")
	private short replica;
	
	
	private boolean isTxnBeanExists(String bean) {
		return beanFactory.containsBean(bean) && beanFactory.isTypeMatch(bean, TransactionService.class);
	}
	private void startComponent(ComponentDef defn) {
		if(isTxnBeanExists(defn.getComponentId())/* && startedComponents.putIfAbsent(defn.getComponentId(), true) == null*/) {
			startConsumers(defn);
		}
		else {
			log.debug("No TransactionService bean found for component - " + defn.getComponentId());
		}
	}
	private void startConsumers(ComponentDef defn) {
		Subscriber commiter = (Subscriber) beanFactory.getBean(ComponentManager.COMMIT_PROCESSOR_BEAN_NAME, defn.getCommitQueue());
		commiter.setCommitLink(defn.getCommitQueueNext());
		commiter.setRollbackLink(defn.getRollbackQueuePrev());
		commiter.setComponentId(defn.getComponentId());
		
		Subscriber rollbacker = null;
		if(StringUtils.hasText(defn.getRollbackQueue())) {
			rollbacker = (Subscriber) beanFactory.getBean(ComponentManager.ROLLBACK_PROCESSOR_BEAN_NAME, defn.getRollbackQueue());
			rollbacker.setRollbackLink(defn.getRollbackQueuePrev());
			rollbacker.setComponentId(defn.getComponentId());
		}
		createTopicsIfNotExist(defn);
		
		if(rollbacker != null)
			rollbacker.run();
		commiter.run();
		
		log.info("Started consumers for transaction component - " + defn.getComponentId());
	}
	private void createTopicsIfNotExist(ComponentDef defn) {
		if(StringUtils.hasText(defn.getCommitQueue()))
			pubAdmin.createTopic(defn.getCommitQueue(), partition, replica);
		
		if(StringUtils.hasText(defn.getCommitQueueNext()))
			pubAdmin.createTopic(defn.getCommitQueueNext(), partition, replica);
		
		if(StringUtils.hasText(defn.getRollbackQueue()))
			pubAdmin.createTopic(defn.getRollbackQueue(), partition, replica);
		
		if(StringUtils.hasText(defn.getRollbackQueuePrev()))
			pubAdmin.createTopic(defn.getRollbackQueuePrev(), partition, replica);
	}

}
