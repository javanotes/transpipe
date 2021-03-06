package org.reactiveminds.txpipe.broker;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.utils.Utils;
import org.reactiveminds.txpipe.broker.KafkaConfiguration.ConsumerOffsetWrapper;
import org.reactiveminds.txpipe.core.api.BrokerAdmin;
import org.reactiveminds.txpipe.err.BrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

class KafkaAdminSupport implements BrokerAdmin {

	private static final Logger log = LoggerFactory.getLogger("KafkaAdminSupport");
	@Autowired
	AdminClient admin;
	@Autowired
	BeanFactory beans;
	@Value("${txpipe.core.instanceId}")
	private String groupId;
	
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
	@Override
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
	@Override
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
	
	@Override
	public Set<String> listConsumers(String topic) {
		try {
			ConsumerOffsetWrapper wrapper = new ConsumerOffsetWrapper();
			admin.listConsumerGroups().valid().get()
					.forEach(c -> wrapper.add(admin.listConsumerGroupOffsets(c.groupId()), c.groupId()));

			return wrapper.getUnwrapped().containsKey(topic) ? wrapper.getUnwrapped().get(topic)
					: Collections.emptySet();

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("While invoking listConsumerGroups", e);
		}
		return Collections.emptySet();
	}

	@Override
	public synchronized long getTotalLag(String topic, String group) {
		try (KafkaTopicIterator iter = beans.getBean(KafkaTopicIterator.class, topic)){

			iter.setGroupId(groupId+".AdminSupport");
			iter.run();
			long end = iter.getEndOffsets().entrySet().stream().mapToLong(e -> e.getValue()).sum();
			long current = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().entrySet()
					.stream().filter(e -> e.getKey().topic().equals(topic)).mapToLong(e -> e.getValue().offset()).sum();

			return end - current;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to fetch total lag for group - " + group, e.getCause());
		}
		return -1;
	}

	@Override
	public int getPartitionCount(String topic) {
		try {
			TopicDescription t = admin.describeTopics(Arrays.asList(topic)).all().get().get(topic);
			if(t != null)
				return t.partitions().size();
			throw new BrokerException("Topic not found - "+topic);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to fetch partitions for topic - " + topic, e.getCause());
		}
		return -1;
	}
	@Override
	public boolean isGroupIdUnique(String group, String topic) {
		if(!listConsumers(topic).contains(group))
			return true;
		try {
			Map<String, ConsumerGroupDescription> allGroups = admin.describeConsumerGroups(Arrays.asList(group)).all().get();
			if (log.isInfoEnabled()) {
				allGroups.forEach((k, v) -> log.info(k + " -> " + v));
			}
			boolean notUniq = allGroups.entrySet()
			.stream().filter(e -> e.getValue().groupId().equals(group))
			.anyMatch(e -> !e.getValue().members().isEmpty());
			return !notUniq;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to describe consumers for group - " + group, e.getCause());
		}
		return false;
	}
	@Override
	public Map<Integer, Long> getPartitionOffset(String topic, String group) {
		try {
			return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().entrySet()
			.stream().filter(e -> e.getKey().topic().equals(topic))
			.collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue().offset()));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new BrokerException("Unable to fetch offsets topic#group - " + topic+" # "+group, e.getCause());
		}
		return Collections.emptyMap();
	}

}
