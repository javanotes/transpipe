package org.reactiveminds.txpipe.core.broker;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.utils.Utils;
import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.reactiveminds.txpipe.err.BrokerException;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.reactiveminds.txpipe.utils.UUIDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.util.Assert;

public class KafkaPublisher implements Publisher {

	private static Event makeEvent(String message, String queue) {
		Event e = new Event();
		e.setDestination(queue);
		e.setPayload(message);
		e.setTxnId(UUIDs.timeBased().toString());
		e.setTimestamp(UUIDs.unixTimestamp(UUID.fromString(e.getTxnId())));
		return e;
	}
	
	private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);
	
	@Autowired
	RequestReplyKafkaTemplate replyKafka;
	
	@Autowired
	AdminClient admin;
	
	@Override
	public String publish(String message, String queue, String pipeline) {
		Event event = makeEvent(message, queue);
		event.setPipeline(pipeline);
		return publish(event);
	}
	/**
	 * extract the pipeline from key.
	 * @param key
	 * @return
	 */
	public static String extractPipeline(String key) {
		int i = -1;
		if((i = key.indexOf(KEY_SEP)) != -1) {
			return key.substring(0,i);
		}
		throw new IllegalArgumentException("Not a valid key '"+key+"'");
	}
	static final char KEY_SEP = '|';
	private String nextKey(Event event) {
		Assert.hasText(event.getPipeline(), "Event pipeline not set " + event);
		Assert.hasText(event.getTxnId(), "Event txnId not set "+ event);
		return event.getPipeline()+KEY_SEP+event.getTxnId();
	}
	
	@Override
	public String publish(Event event) {
		try {
			replyKafka.send(event.getDestination(), nextKey(event), JsonMapper.serialize(event)).get();
			return event.getTxnId();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to publish to Kafka topic", e.getCause());
		}
		return null;
	}

	@Override
	public Future<?> publishAsync(String message, String queue, String pipeline) {
		Event event = makeEvent(message, queue);
		event.setPipeline(pipeline);
		return publishAsync(event);
	}

	@Override
	public Future<?> publishAsync(Event event) {
		return replyKafka.send(event.getDestination(), nextKey(event), JsonMapper.serialize(event));
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
				throw new BrokerException("Topic creation unsuccessful", e.getCause());
			}
		}
		
	}
	/**
	 * Get the partition for the given string key.
	 * @param topic
	 * @param key
	 * @return
	 */
	public int partitionForUtf8Key(String topic, String key) {
		try {
			TopicDescription desc = admin.describeTopics(Arrays.asList(topic)).all().get().get(topic);
			if(desc != null) {
				int len = desc.partitions().size();
				return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % len;
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to fetch topic partition", e.getCause());
		}
		return -1;
	}
	
	private String sendAndReceive(Event event, TimeUnit unit, long timeout) throws TimeoutException {
		try {
			RequestReplyFuture<String, String, String> fut = replyKafka.takePromise(event.getTxnId());
			publish(event);
			return fut.get(timeout, unit).value();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to publish to Kafka topic", e.getCause());
		} catch (TimeoutException e) {
			throw e;
		}
		return null;
	}
	@Override
	public TransactionResult execute(String payload, String queue, String pipeline, long wait, TimeUnit unit) {
		Event event = makeEvent(payload, queue);
		event.setPipeline(pipeline);
		TransactionResult r = TransactionResult.ERROR;
		r.setTxnId(event.getTxnId());
		try {
			String res = sendAndReceive(event, unit, wait);
			r = TransactionResult.valueOf(res);
			r.setTxnId(event.getTxnId());
		} 
		catch (TimeoutException e) {
			log.error(e.getMessage());
			log.debug("", e);
			r = TransactionResult.TIMEOUT;
			r.setTxnId(event.getTxnId());
		}
		catch (Exception e) {
			log.error("", e);
		}
		return r;
	}
}
