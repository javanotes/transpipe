package org.reactiveminds.txpipe.core.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.ComponentDef;
import org.reactiveminds.txpipe.core.PipelineDef;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.Subscriber;
import org.reactiveminds.txpipe.err.ConfigurationException;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.StringUtils;

class DefaultComponentManager implements ComponentManager,AcknowledgingConsumerAwareMessageListener<String,String> {

	static final Logger log = LoggerFactory.getLogger("ComponentManager");
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
	private PartitionAwareMessageListenerContainer container;
	
	@Value("${txpipe.instanceId}")
	private String groupId;
	
	@PostConstruct
	private void onStart() {
		container = beans.getBean(PartitionAwareMessageListenerContainer.class, orchestrationTopic, groupId, 1, new ErrorHandler() {
			
			@Override
			public void handle(Exception t, ConsumerRecord<?, ?> data) {
				log.error("Registry manager error on consume "+data, t);
			}
		});
		container.setupMessageListener(this);
		container.start();
		log.info("Connecting to cluster with instanceId '"+groupId+"'. This can take some time ..");
		
		container.getPartitionListener().awaitOnReady(30, TimeUnit.SECONDS);
		if(container.getPartitionListener().getSnapshot().isEmpty())
			throw new ConfigurationException("No orchestration partitions assigned! Is 'txpipe.instanceId' configured to be unique across cluster?");
		
		AllDefinitionsLoader loader = new AllDefinitionsLoader(orchestrationTopic);
		loader.run();
		loader.allDefinitions.forEach(def -> doPut(def));
		log.info("Loaded all existing definitions into registry ..");
		
		register.values().forEach(p -> startPipeline(p));
	}
	@Autowired
	KafkaAdminSupport adminSupport;
	/**
	 * Load all {@link PipelineDef} saved in Kafka topic as edit log.
	 * @author Sutanu_Dalui
	 *
	 */
	private class AllDefinitionsLoader implements Runnable{

		private final List<PipelineDef> allDefinitions = new ArrayList<>();
		
		private JsonMapper mapper = new JsonMapper();
		private String queryTopic;
		/**
		 * 
		 * @param queryTopic
		 */
		public AllDefinitionsLoader(String queryTopic) {
			super();
			this.queryTopic = queryTopic;
		}

		@Override
		public void run() {
			KafkaTopicIterator iter = null;
			//log.info("Consumers for " + queryTopic + " - " + adminSupport.listConsumers(queryTopic));
			try 
			{
				iter = beans.getBean(KafkaTopicIterator.class, queryTopic);
				iter.run();
				allDefinitions.clear();
				while(iter.hasNext()) {
					List<PipelineDef> items = iter.next().stream().map(s -> mapper.toObject(s, PipelineDef.class)).collect(Collectors.toList());
					allDefinitions.addAll(items);
				}
							
			} 
			catch (Exception e) {
				log.error("Loading of existing definitions failed on startup!", e);
			}
			finally {
				if (iter != null) {
					iter.close();
				}
			}
		}
			
	}
	
	@PreDestroy
	private void destroy() {
		container.stop();
	}
	@Override
	public void add(PipelineDef pipeline) {
		broadcast(pipeline);
	}

	private void startThenPut(PipelineDef def) {
		startPipeline(def);
		doPut(def);
	}
	private void startPipeline(PipelineDef def) {
		def.getComponents().forEach(c -> startComponent(c, def.getPipelineId()));
	}
	private void doPut(PipelineDef def) {
		register.put(def.getPipelineId(), def);
		log.info("Pipeline definition registered [" + def.getPipelineId()+"] (Not all services may be running, however)");
		log.debug(def.toString());
	}
	@Override
	public boolean contains(String pipelineId) {
		return register.containsKey(pipelineId);
	}

	@Override
	public PipelineDef get(String pipelineId) {
		PipelineDef def = register.get(pipelineId);
		if(def != null)
			return new PipelineDef(def.getPipelineId(), def.getComponents());
		
		return null;
	}

	@Override
	public String getSource(String pipelineId) {
		if(contains(pipelineId)) {
			return register.get(pipelineId).getOpeningChannel();
		}
		return null;
	}

	private void broadcast(PipelineDef pipeline) {
		kafkaTemplate.send(orchestrationTopic, mapper.toJson(pipeline));
	}

	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		PipelineDef def = mapper.toObject(data.value(), PipelineDef.class);
		startThenPut(def);
		acknowledgment.acknowledge();
	}
	
	
	//private ConcurrentMap<String, Boolean> startedComponents = new ConcurrentHashMap<>();
	
	@Value("${txpipe.broker.topicPartition:10}")
	private int partition;
	@Value("${txpipe.broker.topicReplica:1}")
	private short replica;
	
	private static class RegisteredProcessor{
		private final Subscriber commit;
		private final Subscriber rollback;
		public RegisteredProcessor(Subscriber commit, Subscriber rollback) {
			super();
			this.commit = commit;
			this.rollback = rollback;
		}
		
		void stop() {
			if(commit != null)
				commit.stop();
			if(rollback != null)
				rollback.stop();
		}
	}
	
	private boolean isTxnBeanExists(String bean) {
		return beanFactory.containsBean(bean) && beanFactory.isTypeMatch(bean, TransactionService.class);
	}
	private void startComponent(ComponentDef defn, String pipe) {
		if(isTxnBeanExists(defn.getComponentId())) {
			startConsumers(defn, pipe);
		}
		else {
			log.debug("No TransactionService bean found for component - " + defn.getComponentId());
		}
	}
	private final Map<String, RegisteredProcessor> processorRegistry = Collections.synchronizedMap(new HashMap<>());
	/**
	 * Prepare and start the consumers.
	 * @param defn
	 * @param pipe
	 */
	private void startConsumers(ComponentDef defn, String pipe) {
		
		Subscriber commitSub = (Subscriber) beanFactory.getBean(ServiceManager.COMMIT_PROCESSOR_BEAN_NAME, defn.getCommitQueue());
		commitSub.setCommitLink(defn.getCommitQueueNext());
		commitSub.setRollbackLink(defn.getRollbackQueuePrev());
		commitSub.setComponentId(defn.getComponentId());
		commitSub.setPipelineId(pipe);
		if(StringUtils.isEmpty(defn.getRollbackQueuePrev())){
			((CommitProcessor) commitSub).setInitialComponent();
		}
		Subscriber rollbackSub = null;
		if(StringUtils.hasText(defn.getRollbackQueue())) {
			rollbackSub = (Subscriber) beanFactory.getBean(ServiceManager.ROLLBACK_PROCESSOR_BEAN_NAME, defn.getRollbackQueue());
			rollbackSub.setRollbackLink(defn.getRollbackQueuePrev());
			rollbackSub.setComponentId(defn.getComponentId());
			rollbackSub.setPipelineId(pipe);
		}
		
		String key = commitSub.getListenerId();
		
		if(processorRegistry.containsKey(key)) {
			log.warn("Running processors for "+ key + " are being stopped on new configuration received ..");
			RegisteredProcessor proc = processorRegistry.remove(key);
			proc.stop();
		}
		
		
		createTopicsIfNotExist(defn);
		
		if(rollbackSub != null) {
			rollbackSub.run();
		}
		commitSub.run();
		processorRegistry.put(key, new RegisteredProcessor(commitSub, rollbackSub));
		
		log.debug(commitSub.toString());
		if (rollbackSub != null) {
			log.debug(rollbackSub.toString());
		}
		
		log.info("Started consumers for transaction component " + key);
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
