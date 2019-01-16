package org.reactiveminds.txpipe.core.broker;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.Command;
import org.reactiveminds.txpipe.core.ComponentDef;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.Subscriber;
import org.reactiveminds.txpipe.core.command.CreatePayload;
import org.reactiveminds.txpipe.core.command.PausePayload;
import org.reactiveminds.txpipe.core.command.ResumePayload;
import org.reactiveminds.txpipe.core.command.StopPayload;
import org.reactiveminds.txpipe.err.ConfigurationException;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.NestedExceptionUtils;
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
	
	
	@Value("${txpipe.broker.orchestrationTopic:managerTopic}") 
	private String orchestrationTopic;
	@Value("${txpipe.broker.loadRegisterOnStart:true}") 
	private boolean loadDefOnStart;
	private ConcurrentMap<String, CreatePayload> register = new ConcurrentHashMap<>();
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
		
		if (loadDefOnStart) {
			AllDefinitionsLoader loader = new AllDefinitionsLoader(orchestrationTopic);
			loader.run();
			loader.allDefinitions.forEach(def -> doPut(def));
			log.info("Loaded all existing definitions into registry ..");
			register.values().forEach(p -> startPipeline(p));
		}
	}
	@Autowired
	KafkaAdminSupport adminSupport;
	
	@PreDestroy
	private void destroy() {
		container.stop();
		ProcessorRegistry.instance().destroy();
	}
	private void startThenPut(CreatePayload def) {
		startPipeline(def);
		doPut(def);
	}
	private void startPipeline(CreatePayload def) {
		def.getComponents().forEach(c -> startComponent(c, def.getPipelineId()));
	}
	private void doPut(CreatePayload def) {
		if (def != null && StringUtils.hasText(def.getPipelineId()) && !def.getComponents().isEmpty()) {
			register.put(def.getPipelineId(), def);
			log.info("Pipeline definition registered [" + def.getPipelineId()
					+ "] (Not all services may be running, however)");
			log.debug(def.toString());
		}
	}
	@Override
	public boolean contains(String pipelineId) {
		return register.containsKey(pipelineId);
	}

	@Override
	public CreatePayload get(String pipelineId) {
		CreatePayload def = register.get(pipelineId);
		if(def != null)
			return new CreatePayload(def.getPipelineId(), def.getComponents());
		
		return null;
	}

	@Override
	public String getSource(String pipelineId) {
		if(contains(pipelineId)) {
			return register.get(pipelineId).getOpeningChannel();
		}
		return null;
	}
	
	private void doStop(Command c) {
		StopPayload cmd = JsonMapper.deserialize(c.getPayload(), StopPayload.class);
		ProcessorRegistry.instance().stop(cmd);
	}
	private void doResume(Command c) {
		ResumePayload cmd = JsonMapper.deserialize(c.getPayload(), ResumePayload.class);
		ProcessorRegistry.instance().resume(cmd);
	}
	private void doPause(Command c) {
		PausePayload cmd = JsonMapper.deserialize(c.getPayload(), PausePayload.class);
		ProcessorRegistry.instance().pause(cmd);
	}
	private void doStart(Command c) {
		CreatePayload def = JsonMapper.deserialize(c.getPayload(), CreatePayload.class);
		startThenPut(def);
	}
	private void switchCommand(String command) {
		Command c = JsonMapper.deserialize(command, Command.class);
		switch(c.getCommand()) {
			case START:
				doStart(c);
				break;
			case PAUSE:
				doPause(c);
				break;
			case RESUME:
				doResume(c);
				break;
			case STOP:
				doStop(c);
				break;
			default:
				log.error("Not a valid command! '"+command+"'. If previous version data lying in orchestration topic, use a new topic");
				break;
		}
	}
	
	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		try {
			switchCommand(data.value());
		} catch(Exception e) {
			log.error(NestedExceptionUtils.buildMessage("Irrecoverable error at component manager ", e));
			log.debug("", e);
		}
		finally {
			acknowledgment.acknowledge();
		}
		
	}
	
	@Value("${txpipe.broker.topicPartition:10}")
	private int partition;
	@Value("${txpipe.broker.topicReplica:1}")
	private short replica;
	
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
		ProcessorRegistry.instance().removeIfPresent(key);
		
		createTopicsIfNotExist(defn);
		
		if(rollbackSub != null) {
			rollbackSub.run();
		}
		commitSub.run();
		ProcessorRegistry.instance().put(key, commitSub, rollbackSub);
		
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
	
	/**
	 * Load all {@link CreatePayload} saved in Kafka topic as edit log.
	 * @author Sutanu_Dalui
	 *
	 */
	private class AllDefinitionsLoader implements Runnable{

		private final List<CreatePayload> allDefinitions = new ArrayList<>();
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
				while (iter.hasNext()) {
					List<CreatePayload> items = iter.next().stream()
							.map(s -> {
									log.debug("Reading meta : " + s);
									try {
										Command c = JsonMapper.deserialize(s, Command.class);
										if(c.isCreate()) {
											return JsonMapper.deserialize(c.getPayload(), CreatePayload.class);
										}
									} catch (UncheckedIOException e) {
										log.debug(e.getMessage() + " (This could due be a mixup of different version metadata lying in orchestration topic)");
										log.debug("", e);
										try {
											return JsonMapper.deserialize(s, CreatePayload.class);
										} catch (UncheckedIOException e1) {
											log.warn(NestedExceptionUtils.buildMessage(
													"Skipping unrecognized metadata read from orchestration topic. This could due be a mixup of different versioned data ",
													e1) );
											log.debug("", e1);
										}
									}
									return null;
							})
							.filter(c -> {
								try {
									return c != null && StringUtils.hasText(c.getOpeningChannel());
								} catch (IllegalArgumentException e) {
									return false;
								}
							})
							.collect(Collectors.toList());
					
					if (items != null && !items.isEmpty()) {
						allDefinitions.addAll(items);
					}
					
				}
							
			} 
			catch (Exception e) {
				log.error("Loading of existing definitions on startup failed! ", e);
			}
			finally {
				if (iter != null) {
					iter.close();
				}
			}
		}
			
	}

}
