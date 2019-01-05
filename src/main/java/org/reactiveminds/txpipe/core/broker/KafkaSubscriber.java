package org.reactiveminds.txpipe.core.broker;

import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.CommitFailedException;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.JsonMapper;
import org.reactiveminds.txpipe.core.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;

abstract class KafkaSubscriber implements Subscriber,AcknowledgingConsumerAwareMessageListener<String,String> {

	static final Logger PLOG = LoggerFactory.getLogger("KafkaSubscriber");
	private static final Logger CLOG = LoggerFactory.getLogger("ContainerErrHandler");
	private final class ContainerErrHandler implements ErrorHandler {
		
		@Override
		public void handle(Exception t, ConsumerRecord<?, ?> data) {
			if (t instanceof CommitFailedException) {
				CLOG.warn("Commit failure from topic " + data.topic() + ", on key [" + data.key() + "] :: " + t.getMessage());
				CLOG.debug(""+data.value(), t);
			} 
			else if (t instanceof UncheckedIOException) {
				CLOG.error("Json parse exception on consuming message from topic " + data.topic() + ", on key [" + data.key() + "] :: " + t.getMessage());
				CLOG.debug(""+data.value(), t);
			} 
			else {
				CLOG.error("Exception on consuming message from topic " + data.topic() + "[" + data.partition()
						+ "] at offset " + data.offset() + ". Key - " + data.key(), t);
				CLOG.debug("Error in consumed message : " + data.value());
			}
			
			recordEvent(data, t);
		}
	}

	@Value("${txpipe.broker.listenerConcurrency:1}")
	private int concurreny;
	@Value("${txpipe.broker.awaitConsumerRebalance:true}")
	private boolean awaitConsumerRebalance;
	@Value("${txpipe.broker.awaitConsumerRebalance.maxWaitSecs:30}")
	private long awaitConsumerRebalanceMaxWait;
	private PartitionAwareMessageListenerContainer container;
	@Autowired
	BeanFactory factory;
	protected boolean isCommitMode = false;
	protected String pipeline;
	@Override
	public void setPipelineId(String pipeline) {
		this.pipeline = pipeline;
	}
	@Override
	public void run() {
		container = (PartitionAwareMessageListenerContainer) factory.getBean("kafkaListenerContainer", listeningTopic, getListenerId(), concurreny, new ContainerErrHandler());
		container.setupMessageListener(this);
		container.start();
		if (awaitConsumerRebalance) {
			boolean done = container.getPartitionListener().awaitOnReady(awaitConsumerRebalanceMaxWait, TimeUnit.SECONDS);
			if(!done)
				PLOG.debug("Container rebalancing did not stabilize in "+awaitConsumerRebalanceMaxWait+" secs .. " + getListenerId());
		}
		PLOG.debug("Container ready .. " + getListenerId());
	}
	protected final String listeningTopic;
	protected KafkaSubscriber(String topic) {
		this.listeningTopic = topic;
	}
	private JsonMapper mapper = new JsonMapper();
	@Override
	public void stop() {
		container.stop();
		PLOG.info("Container stopped .. "+getListenerId());
	}

	@Override
	public String getListenerId() {
		return pipeline+"."+componentId;
	}

	protected String componentId;
	@Override
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	/**
	 * The core process method that should be executed. 
	 * @param event
	 * @return T the returning bean. It should not be {@linkplain Event} type
	 */
	String process(Event event) {
		//the bean should be there, as the starting the components will depend on it
		TransactionService service = (TransactionService) factory.getBean(componentId);
		if(isCommitMode) {
			return service.commit(event.getTxnId(), event.getPayload());
		}
		else {
			service.rollback(event.getTxnId());
		}
		return null;
	}
	private boolean isPassFilter(String key) {
		try {
			return pipeline.equals(KafkaPublisher.extractPipeline(key));
		} catch (Exception e) {
			CLOG.warn("Err on record key filtering - "+e.getMessage());
			CLOG.debug("", e);
		}
		return false;
	}
	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment ack,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {
		
		try {
			if(isPassFilter(data.key())) {
				doOnMessage(data);
			}
		} 
		finally {
			ack.acknowledge();//no message retry
		}
	}
	private void doOnMessage(ConsumerRecord<String, String> data) {
		Event event = mapper.toObject(data.value(), Event.class);
		consume(event);
		recordEvent(data, null);
	}
	/**
	 * 
	 * @param data
	 * @param isError
	 */
	protected void recordEvent(ConsumerRecord<?, ?> data, Exception isError) {
		//TODO: record event
	}
}
