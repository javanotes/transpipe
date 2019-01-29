package org.reactiveminds.txpipe.core;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.broker.ConsumerRecordFilter.PipelineAllowedFilter;
import org.reactiveminds.txpipe.broker.KafkaSubscriber;
import org.reactiveminds.txpipe.broker.KafkaTopicIterator;
import org.reactiveminds.txpipe.broker.PartitionAwareListenerContainer;
import org.reactiveminds.txpipe.core.api.BrokerAdmin;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.Processor;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.dto.Command;
import org.reactiveminds.txpipe.core.dto.ComponentDef;
import org.reactiveminds.txpipe.core.dto.PausePayload;
import org.reactiveminds.txpipe.core.dto.PipelineDef;
import org.reactiveminds.txpipe.core.dto.ResumePayload;
import org.reactiveminds.txpipe.core.dto.StopPayload;
import org.reactiveminds.txpipe.err.TxPipeConfigurationException;
import org.reactiveminds.txpipe.err.TxPipeIntitializationException;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.util.StringUtils;

class DefaultComponentManager extends KafkaSubscriber implements ComponentManager{

	DefaultComponentManager(String topic) {
		super(topic);
		atMostOnceDelivery = false;
		awaitConsumerRebalance = true;
		awaitConsumerRebalanceMaxWait = 30;
		concurreny = 1;
	}

	static final Logger log = LoggerFactory.getLogger("ComponentManager");
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	ConsumerFactory<String, String> consumerFactory;
	@Autowired
	BeanFactory beanFactory;
	@Autowired
	Publisher pubAdmin;
	@Autowired
	BrokerAdmin admin;
	
	
	@Value("${txpipe.core.loadRegisterOnStart:true}") 
	private boolean loadDefOnStart;
	private ConcurrentMap<String, PipelineDef> register = new ConcurrentHashMap<>();
	private PartitionAwareListenerContainer abortListener;
	@Value("${txpipe.core.readAbortOnStartup.maxWaitSecs:60}")
	private long readAbortWait;
	
	@Value("${txpipe.core.instanceId}")
	private String groupId;
	private CountDownLatch startupLatch;
	private String abortTopic() {
		return getListeningTopic()+ServiceManager.ABORT_TOPIC_SUFFIX;
	}
	private void startAbortListener() {
		int abortLag = (int) admin.getTotalLag(abortTopic(), groupId);
		startupLatch = new CountDownLatch(abortLag);
		abortListener = beanFactory.getBean(PartitionAwareListenerContainer.class, abortTopic(), groupId, 1, new ErrorHandler() {
			
			@Override
			public void handle(Exception t, ConsumerRecord<?, ?> data) {
				log.error("Error on abort consume "+data, t);
			}
		});
		abortListener.setupMessageListener(this);
		abortListener.start();
		try {
			boolean b = startupLatch.await(readAbortWait, TimeUnit.SECONDS);
			if(!b)
				throw new TxPipeIntitializationException(
						"Abort listener did not complete in time. This can cause aborted transactions to fire unexpectedly. Restart node until this error goes away");
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	private void startContainer() {
		startAbortListener();
		log.info("Joining cluster with instanceId '"+groupId+"' ..");
		run();
		if (isPartitionUnassigned())
			throw new TxPipeConfigurationException(
					"No orchestration partitions assigned! Is 'txpipe.core.instanceId' configured to be unique across cluster? You may consider try after sometime to allow some time to balance");
	}
	private void loadMetadata() {
		if (loadDefOnStart) {
			log.info("Start reading existing metadata ..");
			AllDefinitionsLoader loader = new AllDefinitionsLoader(getListeningTopic());
			try {
				loader.run();
				loader.allDefinitions.forEach(def -> doPut(def));
				log.info("Loaded discovered definitions into registry");
			} finally {
				
			}
		}
	}
	private void startPipelines() {
		register.values().forEach(p -> startPipeline(p, true));
	}
	private void verifyOTPartitions() {
		admin.createTopic(getListeningTopic(), 1, (short) 1);
		int c = admin.getPartitionCount(getListeningTopic());
		if(c != 1)
			throw new TxPipeConfigurationException(getListeningTopic() +" should be a single partition topic. You may consider try after sometime to allow some time to balance");
	}
	private void verifyInstanceUnique() {
		if(!admin.isGroupIdUnique(groupId, getListeningTopic()))
			throw new TxPipeConfigurationException("'txpipe.core.instanceId' should configured to be unique across cluster. You may consider try after sometime to allow some time to balance");
	}
	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		verifyOTPartitions();
		verifyInstanceUnique();
		loadMetadata();
		startContainer();
		//start after processing all pending commands
		startPipelines();
	}
	
	@Override
	public void destroy() {
		abortListener.stop();
		super.destroy();
		ProcessorRegistry.instance().destroy();
	}
	private void startThenPut(PipelineDef def) {
		startPipeline(def, false);
		doPut(def);
	}
	private void startPipeline(PipelineDef def, boolean startIfNotStarted) {
		def.getComponents().forEach(c -> startComponent(c, def, startIfNotStarted));
	}
	private void doPut(PipelineDef def) {
		if (def != null && StringUtils.hasText(def.getPipelineId()) && !def.getComponents().isEmpty()) {
			register.put(def.getPipelineId(), def);
			log.debug("Pipeline definition registered [" + def.getPipelineId()
					+ "] (Not all services may be running, however)");
			log.debug(def.toString());
		}
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
		PipelineDef def = JsonMapper.deserialize(c.getPayload(), PipelineDef.class);
		startThenPut(def);
	}
	private void doAbort(Command c) {
		ProcessorRegistry.instance().abort(c.getPayload());
	}
	private void switchCommand(Command c) {
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
				log.error("Not a valid command! '"+c+"'. If previous version data lying in orchestration topic, use a new topic");
				break;
		}
	}
	
	@Value("${txpipe.broker.topicPartition:10}")
	private int partition;
	@Value("${txpipe.broker.topicReplica:1}")
	private short replica;
	
	private boolean isTxnBeanExists(String bean) {
		return beanFactory.containsBean(bean) && beanFactory.isTypeMatch(bean, TransactionService.class);
	}
	private void startComponent(ComponentDef defn, PipelineDef pipe, boolean startIfNotStarted) {
		if(isTxnBeanExists(defn.getComponentId())) {
			startConsumers(defn, pipe, startIfNotStarted);
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
	private void startConsumers(ComponentDef defn, PipelineDef pipe, boolean startIfNotStarted) {
		Processor commitSub = (Processor) beanFactory.getBean(ServiceManager.COMMIT_PROCESSOR_BEAN_NAME, defn.getCommitQueue());
		commitSub.setCommitLink(defn.getCommitQueueNext());
		commitSub.setRollbackLink(defn.getRollbackQueuePrev());
		commitSub.setComponentId(defn.getComponentId());
		commitSub.setPipelineId(pipe.getPipelineId());
		commitSub.addFilter(new PipelineAllowedFilter(pipe.getPipelineId()));
		if(StringUtils.isEmpty(defn.getRollbackQueuePrev())){
			((CommitProcessor) commitSub).setInitialComponent();
			((CommitProcessor) commitSub).setPipelineExpiryMillis(pipe.getExpiryMillis());
		}
		final String key = commitSub.getListenerId();
		if(startIfNotStarted && ProcessorRegistry.instance().isAlreadyPresent(key)) {
			log.info("Not restarting "+key);
			return;
		}
		Processor rollbackSub = null;
		if(StringUtils.hasText(defn.getRollbackQueue())) {
			rollbackSub = (Processor) beanFactory.getBean(ServiceManager.ROLLBACK_PROCESSOR_BEAN_NAME, defn.getRollbackQueue());
			rollbackSub.setRollbackLink(defn.getRollbackQueuePrev());
			rollbackSub.setComponentId(defn.getComponentId());
			rollbackSub.setPipelineId(pipe.getPipelineId());
			rollbackSub.addFilter(new PipelineAllowedFilter(pipe.getPipelineId()));
		}
		
		
		createTopicsIfNotExist(defn);
		ProcessorRegistry.instance().removeIfPresent(key);
		
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
			admin.createTopic(defn.getCommitQueue(), partition, replica);
		
		if(StringUtils.hasText(defn.getCommitQueueNext()))
			admin.createTopic(defn.getCommitQueueNext(), partition, replica);
		
		if(StringUtils.hasText(defn.getRollbackQueue()))
			admin.createTopic(defn.getRollbackQueue(), partition, replica);
		
		if(StringUtils.hasText(defn.getRollbackQueuePrev()))
			admin.createTopic(defn.getRollbackQueuePrev(), partition, replica);
	}
	
	/**
	 * Load all {@link PipelineDef} saved in Kafka topic as edit log.
	 * @author Sutanu_Dalui
	 *
	 */
	private class AllDefinitionsLoader implements Runnable{

		private final List<PipelineDef> allDefinitions = new ArrayList<>();
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
			try (KafkaTopicIterator iter = beanFactory.getBean(KafkaTopicIterator.class, queryTopic))
			{
				iter.setGroupId(groupId+".DefinitionsLoader");
				iter.run();
				allDefinitions.clear();
				while (iter.hasNext()) {
					List<PipelineDef> items = iter.next().stream()
							.map(s -> {
									log.debug("Reading meta : " + s);
									try {
										Command c = JsonMapper.deserialize(s, Command.class);
										if(c.isCreate()) {
											return JsonMapper.deserialize(c.getPayload(), PipelineDef.class);
										}
									} catch (UncheckedIOException e) {
										log.debug(e.getMessage() + " (This could due be a mixup of different version metadata lying in orchestration topic)");
										log.debug("", e);
										try {
											return JsonMapper.deserialize(s, PipelineDef.class);
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
		}
			
	}

	@Override
	public String getListenerId() {
		return groupId;
	}
	@Override
	protected void processNext(ConsumerRecord<String, String> data) {
		try 
		{
			Command c = JsonMapper.deserialize(data.value(), Command.class);
			if(data.topic().equals(abortTopic())) {
				startupLatch.countDown();
				doAbort(c);
			}
			else
				switchCommand(c);
			
		} catch(Exception e) {
			log.error("Irrecoverable error at component manager ", e);
		}
	}
}
