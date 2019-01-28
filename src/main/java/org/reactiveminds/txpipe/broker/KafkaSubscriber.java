package org.reactiveminds.txpipe.broker;

import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.Subscriber;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.dto.Event;
import org.reactiveminds.txpipe.core.dto.TransactionState;
import org.reactiveminds.txpipe.err.TxPipeIntitializationException;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;
import org.reactiveminds.txpipe.spi.EventRecord;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.spi.PayloadCodec;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.StringUtils;

abstract class KafkaSubscriber implements Subscriber,AcknowledgingConsumerAwareMessageListener<String,String> {

	static final Logger PLOG = LoggerFactory.getLogger("KafkaSubscriber");
	private static final Logger CLOG = LoggerFactory.getLogger("CommitFailureLogger");
	private static final Logger OLOG = LoggerFactory.getLogger("ContainerErrLogger");
	private final class ContainerErrHandler implements ErrorHandler {
		
		@Override
		public void handle(Exception t, ConsumerRecord<?, ?> data) {
			if (t instanceof CommitFailedException) {
				CLOG.warn("From " + data.topic() + ", on key [" + data.key() + "] :: " + t.getMessage());
				CLOG.debug(""+data.value(), t);
			} 
			else if (t instanceof UncheckedIOException) {
				OLOG.error("Json parse exception on consuming message from topic " + data.topic() + ", on key [" + data.key() + "] :: " + t.getMessage());
				OLOG.debug(""+data.value(), t);
			} 
			else {
				OLOG.warn("Unhandled exception from topic " + data.topic() + "[" + data.partition()
						+ "] at offset " + data.offset() + ". Key - " + data.key(), t);
				OLOG.debug("Error in consumed message : " + data.value());
			}
			recordEvent(data, t);
		}
	}
	/**
	 * 
	 * @author Sutanu_Dalui
	 *
	 */
	private class EventRecorderRunner implements Runnable{

		private EventRecorderRunner(ConsumerRecord<?, ?> data, Exception isError) {
			super();
			this.data = data;
			this.isError = isError;
		}
		private final ConsumerRecord<?, ?> data;
		private final Exception isError;
		/**
		 * record the event if needed for further tracing/analysis.
		 * @param data
		 * @param isError
		 */
		protected void recordEvent() {
			if (eventRecorder != null) {
				EventRecord record = new EventRecord(data.topic(), data.partition(), data.offset(), data.timestamp(),
						data.key() != null ? data.key().toString() : "");
				record.setError(isError != null);
				if (record.isError()) {
					record.setErrorDetail(isError.getMessage());
				}
				record.setValue(data.value() != null ? data.value().toString() : "");
				record.setRollback(!isCommitMode);
				eventRecorder.record(record);
			}
		}
		@Override
		public void run() {
			recordEvent();
		}
		
	}
	@Value("${txpipe.broker.listenerConcurrency:1}")
	private int concurreny;
	@Value("${txpipe.broker.awaitConsumerRebalance:true}")
	private boolean awaitConsumerRebalance;
	@Value("${txpipe.broker.awaitConsumerRebalance.maxWaitSecs:30}")
	private long awaitConsumerRebalanceMaxWait;
	@Value("${txpipe.event.recorder.async:true}")
	private boolean recordEventAsync;
	@Value("${txpipe.event.recorder.async.maxThreads:2}")
	private int recordEventAsyncMaxThreads;
	@Value("${txpipe.broker.recordExpirySecs:60}")
	private long recordExpirySecs;
	@Value("${txpipe.core.discoveryAgent:}")
	private String discoveryService;
	
	private PartitionAwareMessageListenerContainer container;
	@Autowired
	BeanFactory factory;
	
	private EventRecorder eventRecorder;
	private boolean isRecordEventEnabled;
	
	protected boolean isCommitMode = false;
	protected String pipeline;
	@Override
	public void setPipelineId(String pipeline) {
		this.pipeline = pipeline;
	}
	@Autowired
	KafkaTemplate<String, String> pub;
	private DiscoveryAgent serviceLocator;
	
	private ExecutorService eventThread;
	private volatile boolean started = false;
	@Override
	public void run() {
		try {
			eventRecorder = factory.getBean(EventRecorder.class);
			isRecordEventEnabled = true;
		} catch (BeansException e1) {
			isRecordEventEnabled = false;
			PLOG.warn("EventRecorder was not configured");
		}
		if(isRecordEventEnabled && recordEventAsync) {
			eventThread = Executors.newFixedThreadPool(recordEventAsyncMaxThreads, (r)-> new Thread(r, "SubcriberEventRecorder"));
		}
		addFilter(k -> pipeline.equals(KafkaPublisher.extractPipeline(k.key())));
		addFilter(k -> System.currentTimeMillis() - k.timestamp() < TimeUnit.SECONDS.toMillis(recordExpirySecs));
		try {
			serviceLocator = StringUtils.hasText(discoveryService) ? (DiscoveryAgent) Class.forName(discoveryService).newInstance() : factory.getBean(DiscoveryAgent.class);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			PLOG.error("Discovery class not loaded", e);
			throw new TxPipeIntitializationException("Discovery class not loaded", e);
		}
		
		container = factory.getBean(PartitionAwareMessageListenerContainer.class, listeningTopic, getListenerId(), concurreny, new ContainerErrHandler());
		container.setupMessageListener(this);
		container.setBeanName(getListenerId());
		container.start();
		started = true;
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
	@Override
	public void stop() {
		container.stop();
		started = false;
		PLOG.info("Container stopped .. "+getListenerId());
		if(eventThread != null) {
			eventThread.shutdown();
			try {
				eventThread.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	static final char LISTENER_ID_SEP = '.';
	@Override
	public String getListenerId() {
		return pipeline+LISTENER_ID_SEP+componentId;
	}

	protected String componentId;
	@Override
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	@Autowired
	private TransactionMarker marker;
	final void beginTxn(String txnId) {
		marker.begin(txnId);
	}
	final void endTxn(String txnId, boolean success) {
		marker.end(txnId, success);
		pub.send(ComponentManager.TXPIPE_REPLY_TOPIC, txnId, success ? TransactionResult.COMMIT.name() : TransactionResult.ROLLBACK.name());
	}
	/**
	 * The core process method that should be executed. 
	 * @param event
	 * @return T the returning bean. It should not be {@linkplain Event} type
	 */
	final String process(Event event) {
		//the bean should be there, as the starting the components will depend on it
		TransactionService service = serviceLocator.getServiceById(componentId);
		String response = null;
		
		TransactionState state = new TransactionState();
		state.setComponent(componentId);
		state.setPipeline(pipeline);
		state.setSequence((short) event.getEventId());
		state.setTransactionId(event.getTxnId());
		state.setTimestamp(event.getTimestamp());
		
		if(isCommitMode) {
			response = service.commit(event.getTxnId(), codec.decode(event.getPayload()));
			state.setCommit(true);
			marker.update(state);
		}
		else {
			service.rollback(event.getTxnId());
			state.setCommit(false);
			marker.update(state);
		}
		return response;
	}
	@Autowired
	PayloadCodec codec;
	/**
	 * Abort on request timeout
	 * @param event
	 */
	final void abort(Event event) {
		//the bean should be there, as the starting the components will depend on it
		TransactionService service = (TransactionService) factory.getBean(componentId);
		try {
			service.abort(event.getTxnId());
		} finally {
			recordEvent(event, new CommitFailedException("Rolled back on timeout", null));
		}
	}
	private volatile Predicate<ConsumerRecord<String, String>> filters;
	/**
	 * Add another {@linkplain AllowedTransactionFilter} AND-ed to existing, if any.
	 * @param f
	 */
	synchronized void addFilter(AllowedTransactionFilter f) {
		filters = filters == null ? f : filters.and(f);
	}
	/**
	 * Test all the filters to determine if this message will be delivered for processing
	 * @param key
	 * @return
	 */
	private boolean isPassFilter(ConsumerRecord<String, String> data) {
		try {
			return filters.test(data);
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
			if(isPassFilter(data)) {
				doOnMessage(data);
			}
		} 
		finally {
			ack.acknowledge();//no message retry as this is a transactional system
		}
	}
	private void doOnMessage(ConsumerRecord<String, String> data) {
		Event event = JsonMapper.deserialize(data.value(), Event.class);
		Exception ex = null;
		try {
			consume(event);
		} catch (Exception e) {
			ex = e;
			throw e;
		}
		finally {
			recordEvent(data, ex);
		}
	}
	private void recordEvent(ConsumerRecord<?, ?> data, Exception e) {
		if (isRecordEventEnabled) {
			if (recordEventAsync) {
				eventThread.submit(new EventRecorderRunner(data, e));
			} else
				new EventRecorderRunner(data, e).run();
		}
	}
	/**
	 * To record event on a forced rollback.
	 * @param event
	 * @param e
	 */
	private void recordEvent(Event event, Exception e) {
		if (isRecordEventEnabled) {
			recordEvent(new ConsumerRecord<>(event.getDestination(), -1, -1, event.getTimestamp(),
					TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, event.getTxnId(), event.getPayload()), e);
		}
	}
	private volatile boolean paused = false;
	@Override
	public void pause() {
		container.pause();
		paused = true;
		PLOG.warn("Container paused .. "+getListenerId());
	}
	@Override
	public void resume() {
		container.resume();
		paused = false;
		PLOG.info("Container resumed .. "+getListenerId());
	}
	@Override
	public boolean isPaused() {
		return paused;
	}
	@Override
	public boolean isRunning() {
		return started;
	}
}
