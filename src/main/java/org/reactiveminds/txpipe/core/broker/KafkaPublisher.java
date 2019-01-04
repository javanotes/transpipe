package org.reactiveminds.txpipe.core.broker;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.JsonMapper;
import org.reactiveminds.txpipe.core.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaPublisher implements Publisher {

	private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);
	@Autowired
	KafkaTemplate<String, String> kafka;
	@Autowired
	AdminClient admin;
	private JsonMapper mapper = new JsonMapper();
	
	@Override
	public <T> String publish(String message, String queue, String pipeline, String txnId) {
		Event event = Publisher.makeEvent(message, queue, txnId);
		event.setPipeline(pipeline);
		return publish(event);
	}
	
	private String nextKey(Event event) {
		return event.getPipeline()+"|"+event.getTxnId();
	}
	@SuppressWarnings("serial")
	@Override
	public String publish(Event event) {
		try {
			kafka.send(event.getDestination(), nextKey(event), mapper.toJson(event)).get();
			return event.getTxnId();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new DataAccessException("Unable to publish to Kafka topic", e.getCause()) {};
		}
		return null;
	}

	@Override
	public Future<?> publishAsync(String message, String queue, String pipeline, String txnId) {
		Event event = Publisher.makeEvent(message, queue, txnId);
		event.setPipeline(pipeline);
		return publishAsync(event);
	}

	@Override
	public Future<?> publishAsync(Event event) {
		return kafka.send(event.getDestination(), nextKey(event), mapper.toJson(event));
	}
	private boolean isTopicExists(String topic) {
		ListTopicsOptions l = new ListTopicsOptions();
		l.listInternal(false);
		try {
			return admin.listTopics(l).names().get(30, TimeUnit.SECONDS).contains(topic);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			log.warn("", e);
		}
		return false; 
	}
	/**
	 * Create topic
	 * @param topicName
	 * @param partition
	 * @param replica
	 */
	public void createTopic(String topicName, int partition, short replica) {
		ListTopicsOptions l = new ListTopicsOptions();
		l.listInternal(false);
		if(!isTopicExists(topicName)) {
			try {
				admin.createTopics(Arrays.asList(new NewTopic(topicName, partition, replica))).all().get();
				log.info("Topic created - "+topicName);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				log.error("Topic creation unsuccessful!", e.getCause());
			}
		}
		
	}

}
