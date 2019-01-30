package org.reactiveminds.txpipe.core.api;

import java.util.Map;
import java.util.Set;

public interface BrokerAdmin {

	/**
	 * List consumers for a given topic. This operation will browse through all available consumers
	 * 
	 * @param topic
	 * @return
	 */
	Set<String> listConsumers(String topic);
	/**
	 * 
	 * @param group
	 * @param topic
	 * @return
	 */
	boolean isGroupIdUnique(String group, String topic);
	/**
	 * Get the current lag for a given groupId on the topic
	 * @param topic
	 * @param group
	 * @return
	 */
	long getTotalLag(String topic, String group);
	Map<Integer, Long> getPartitionOffset(String topic, String group);
	/**
	 * 
	 * @param topic
	 * @return
	 */
	int getPartitionCount(String topic);
	/**
	 * 
	 * @param topic
	 * @param key
	 * @return
	 */
	int partitionForUtf8Key(String topic, String key);
	/**
	 * 
	 * @param topicName
	 * @param partition
	 * @param replica
	 */
	void createTopic(String topicName, int partition, short replica);
}