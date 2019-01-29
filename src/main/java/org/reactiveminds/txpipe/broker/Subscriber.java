package org.reactiveminds.txpipe.broker;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
/**
 * Interface to define a standard Kafka subscriber.
 * @author Sutanu_Dalui
 *
 */
public interface Subscriber extends AcknowledgingConsumerAwareMessageListener<String,String>,Runnable{
	/**
	 * 
	 * @return
	 */
	boolean isPaused();
	/**
	 * 
	 */
	void pause();
	/**
	 * 
	 */
	void resume();
	/**
	 * 
	 * @return
	 */
	String getListenerId();

	/**
	 * 
	 * @return
	 */
	boolean isRunning();

	/**
	 * 
	 * @return
	 */
	String getListeningTopic();
	/**
	 * 
	 * @param f
	 */
	void addFilter(ConsumerRecordFilter f);
}