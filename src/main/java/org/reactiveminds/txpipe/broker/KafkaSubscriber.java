package org.reactiveminds.txpipe.broker;

import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.broker.ConsumerRecordFilter.RecordExpiredFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;

public abstract class KafkaSubscriber implements Subscriber,InitializingBean,DisposableBean {

	protected static final Logger PLOG = LoggerFactory.getLogger("KafkaSubscriber");
	private static final Logger CLOG = LoggerFactory.getLogger("CommitFailureLogger");
	private static final Logger OLOG = LoggerFactory.getLogger("ContainerErrLogger");
	/**
	 * 
	 * @author Sutanu_Dalui
	 *
	 */
	protected class ContainerErrHandler implements ErrorHandler {
		
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
		}
	}
	
	@Value("${txpipe.broker.listenerConcurrency:1}")
	protected int concurreny;
	@Value("${txpipe.broker.awaitConsumerRebalance:true}")
	protected boolean awaitConsumerRebalance;
	@Value("${txpipe.broker.awaitConsumerRebalance.maxWaitSecs:30}")
	protected long awaitConsumerRebalanceMaxWait;
	@Value("${txpipe.broker.recordExpirySecs:60}")
	protected long recordExpirySecs;
	
	protected volatile boolean atMostOnceDelivery = true;
	private PartitionAwareListenerContainer container;
	@Autowired
	private ConfigurableApplicationContext factory;
	
	protected boolean isPartitionUnassigned() {
		return container.getPartitionListener().getSnapshot().isEmpty();
	}
	
	private volatile boolean started = false;
	@Override
	public synchronized void addFilter(ConsumerRecordFilter f) {
		filters = filters == null ? f : filters.and(f);
	}
	@Override
	public void afterPropertiesSet() throws Exception{
		addFilter(new RecordExpiredFilter(TimeUnit.SECONDS.toMillis(recordExpirySecs)));
	}
	/**
	 * Return a consumer {@linkplain ErrorHandler} to process.
	 * @return
	 */
	protected ErrorHandler errorHandler() {
		return new ContainerErrHandler();
	}
	private void start() {
		container = factory.getBean(PartitionAwareListenerContainer.class, getListeningTopic(),
				getListenerId(), concurreny, errorHandler());
		container.setupMessageListener(this);
		container.setBeanName(getListenerId());
		container.start();
		started = true;
		if (awaitConsumerRebalance) {
			boolean done = container.getPartitionListener().awaitOnReady(awaitConsumerRebalanceMaxWait,
					TimeUnit.SECONDS);
			if (!done)
				PLOG.debug("Container rebalancing did not stabilize in " + awaitConsumerRebalanceMaxWait
						+ " secs .. " + getListenerId());
		}
		PLOG.debug("Container ready .. " + getListenerId());
	}
	@Override
	public final void run() {
		if (!started) {
			synchronized (this) {
				if (!started) {
					start();
				}
			} 
		}
	}
	private final String listeningTopic;
	/**
	 * 
	 * @param topic
	 */
	protected KafkaSubscriber(String topic) {
		this.listeningTopic = topic;
	}
	@Override
	public void destroy() {
		container.stop();
		started = false;
		PLOG.info("Container stopped .. "+getListenerId());
	}

	private volatile Predicate<ConsumerRecord<String, String>> filters = ConsumerRecordFilter.ALLOW_ALL;
	
	/**
	 * Test all the filters to determine if this message should be delivered for processing
	 * @param key
	 * @return
	 */
	private boolean isPassFilter(ConsumerRecord<String, String> data) {
		try {
			return filters.test(data);
		} catch (Exception e) {
			CLOG.warn("Error on record key filtering - "+e.getMessage());
			CLOG.debug("", e);
		}
		return false;
	}
	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment ack,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {
		boolean ackDone =false;
		if (atMostOnceDelivery) {
			ack.acknowledge();
			ackDone = true;
		}
		try {
			if (isPassFilter(data)) {
				processNext(data);
			} 
		} finally {
			if(!ackDone)
				ack.acknowledge();
		}
	}
	/**
	 * Process the record after all filtration have been done.
	 * @param data
	 */
	protected abstract void processNext(ConsumerRecord<String, String> data);
	@Override
	public boolean isRunning() {
		return started;
	}
	@Override
	public String getListeningTopic() {
		return listeningTopic;
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
	
}
