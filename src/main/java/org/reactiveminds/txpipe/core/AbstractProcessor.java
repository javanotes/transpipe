package org.reactiveminds.txpipe.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.broker.KafkaSubscriber;
import org.reactiveminds.txpipe.core.api.Processor;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.dto.Event;
import org.reactiveminds.txpipe.core.dto.TransactionState;
import org.reactiveminds.txpipe.err.TxPipeIntitializationException;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;
import org.reactiveminds.txpipe.spi.EventRecord;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.spi.PayloadCodec;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.util.StringUtils;

abstract class AbstractProcessor extends KafkaSubscriber implements Processor {

	protected AbstractProcessor(String topic) {
		super(topic);
	}
	@Override
	protected ErrorHandler errorHandler() {
		return new ComponentErrHandler();
	}
	private final class ComponentErrHandler extends KafkaSubscriber.ContainerErrHandler {
		
		@Override
		public void handle(Exception t, ConsumerRecord<?, ?> data) {
			super.handle(t, data);
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
	
	@Value("${txpipe.event.recorder.async:true}")
	private boolean recordEventAsync;
	@Value("${txpipe.event.recorder.async.maxThreads:2}")
	private int recordEventAsyncMaxThreads;
	
	@Value("${txpipe.core.discoveryAgent:}")
	private String discoveryService;
	
	@Autowired
	ConfigurableApplicationContext factory;
	
	private EventRecorder eventRecorder;
	private boolean isRecordEventEnabled;
	
	protected boolean isCommitMode = false;
	protected String pipeline;
	@Override
	public void setPipelineId(String pipeline) {
		this.pipeline = pipeline;
	}
	private DiscoveryAgent serviceLocator;
	
	private ExecutorService eventThread;
	
	@Override
	public void afterPropertiesSet() throws Exception{
		super.afterPropertiesSet();
		try {
			eventRecorder = factory.getBean(EventRecorder.class);
			isRecordEventEnabled = true;
		} catch (BeansException e1) {
			isRecordEventEnabled = false;
			PLOG.debug("EventRecorder was not configured");
		}
		if(isRecordEventEnabled && recordEventAsync) {
			eventThread = Executors.newFixedThreadPool(recordEventAsyncMaxThreads, (r)-> new Thread(r, "SubcriberEventRecorder"));
		}
		try {
			serviceLocator = StringUtils.hasText(discoveryService) ? (DiscoveryAgent) Class.forName(discoveryService).newInstance() : factory.getBean(DiscoveryAgent.class);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			PLOG.error("Discovery class not loaded", e);
			throw new TxPipeIntitializationException("Discovery class not loaded", e);
		}
	}
	
	@Override
	public void destroy() {
		super.destroy();
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
	protected TransactionMarker txnMarker;
	
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
			txnMarker.update(state);
		}
		else {
			service.rollback(event.getTxnId());
			state.setCommit(false);
			txnMarker.update(state);
		}
		return response;
	}
	@Autowired
	protected PayloadCodec codec;
	/**
	 * Abort on request timeout
	 * @param event
	 */
	@Override
	public final void abort(Event event) {
		//the bean should be there, as the starting the components will depend on it
		TransactionService service = (TransactionService) factory.getBean(componentId);
		try {
			service.abort(event.getTxnId());
		} finally {
			recordEvent(event, new CommitFailedException("Rolled back on timeout", null));
		}
	}
	
	private void process(ConsumerRecord<String, String> data) {
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
	@Override
	protected void processNext(ConsumerRecord<String, String> data) {
		process(data);
	}
}
