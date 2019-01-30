package org.reactiveminds.txpipe.broker;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.reactiveminds.txpipe.core.api.BrokerAdmin;
import org.reactiveminds.txpipe.err.BrokerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
/**
 * Utility class to use Kafka topics as an edit log. Thus enabling to use Kafka as a simple key,value datastore.
 * Consequently this is to be used for comparatively small queues, since all data will be fetched into memory. Consider
 * using {@link #setQueryKeyLike(Predicate)} to provide a filter criteria. <p><b>Note: </b> It is important to understand, if
 * the usage needs to be like a table query - the underlying topic items should be in order.
 * @author Sutanu_Dalui
 *
 */
public class KafkaTopicIterator implements Runnable, Iterator<List<String>>, AutoCloseable{

	private Map<TopicPartition, Long> endOffsets;
	private Consumer<String, String> consumer;
	@Autowired
	ConsumerFactory<String, String> consumerFactory;
	@Autowired
	BrokerAdmin adminSupport;
	private String queryTopic;
	//match all by default
	private Predicate<String> queryKeyLike = s -> true;
	/**
	 * 
	 * @param queryTopic
	 */
	public KafkaTopicIterator(String queryTopic) {
		super();
		this.queryTopic = queryTopic;
	}
	private String groupId, clientId;
	private boolean preserveOrder = false;
	private volatile boolean initRan = false;
	private boolean readUnconsumed = false;
	
	@Override
	public void run() {

		try {
			consumer = consumerFactory.createConsumer(
					StringUtils.hasText(getGroupId()) ? getGroupId() : UUID.randomUUID().toString(),
					StringUtils.hasText(getClientId()) ? getClientId() : "-client");
			
			List<TopicPartition> topicParts = consumer.partitionsFor(queryTopic).stream()
					.map(p -> new TopicPartition(queryTopic, p.partition())).collect(Collectors.toList());

			consumer.assign(topicParts);
			if(readUnconsumed) {
				Assert.isTrue(StringUtils.hasText(getGroupId()), "'groupId' needs to be specified to iterate unconsumed records");
				adminSupport.getPartitionOffset(queryTopic, getGroupId())
						.forEach((k, v) -> consumer.seek(new TopicPartition(queryTopic, k), v));
			}
			else
				consumer.seekToBeginning(consumer.assignment());
			
			endOffsets = consumer.endOffsets(consumer.assignment());
			initRan = true;
			
		} catch (Exception e) {
			throw new BrokerException("Initialization of iterator failed", e);
		}

	}
	
	public Map<TopicPartition, Long> getEndOffsets() {
		return Collections.unmodifiableMap(endOffsets);
	}

	private boolean hasPendingMessages() {
		return endOffsets.entrySet().stream().anyMatch(e -> e.getValue() > consumer.position(e.getKey()));
	}

	public Predicate<String> getQueryKeyLike() {
		return queryKeyLike;
	}

	public void setQueryKeyLike(Predicate<String> queryKeyLike) {
		this.queryKeyLike = queryKeyLike;
	}

	@Override
	public boolean hasNext() {
		return initRan && hasPendingMessages();
	}

	@Override
	public List<String> next() {
		if(!initRan)
			throw new IllegalStateException("This iterator has to be run() once, before attempting to iterate");
		if (hasNext()) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			consumer.commitSync();
			return StreamSupport.stream(records.spliterator(), !preserveOrder).filter(c -> queryKeyLike.test(c.key()))
					.map(c -> c.value()).collect(Collectors.toList());
		}
		else
			throw new NoSuchElementException();
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.close();
		}
	}

	public boolean isPreserveOrder() {
		return preserveOrder;
	}
	/**
	 * If set true, preserves the order by not using a parallelStream() invocation
	 * @param preserveOrder
	 */
	public void setPreserveOrder(boolean preserveOrder) {
		this.preserveOrder = preserveOrder;
	}
	/**
	 * Will iterate from latest offset, that is unconsumed records only. Equivalent 
	 * to auto.offset.reset=latest
	 */
	public void offsetResetLatest() {
		readUnconsumed = true;
	}
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	
}