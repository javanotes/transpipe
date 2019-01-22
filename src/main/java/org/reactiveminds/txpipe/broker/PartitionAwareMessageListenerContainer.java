package org.reactiveminds.txpipe.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

final class PartitionAwareMessageListenerContainer extends ConcurrentMessageListenerContainer<String, String>{

	public static class PartitionListener extends Observable implements ConsumerRebalanceListener{
		private static final Logger log = LoggerFactory.getLogger("PartitionListener");
		private final Map<String, Set<TopicPartition>> snapshot = new HashMap<>();
		/**
		 * 
		 * @param partitions
		 * @return
		 */
		private static Map<String, Set<TopicPartition>> streamToMap(Collection<TopicPartition> partitions){
			return partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic, Collectors.toSet()));
		}
		public PartitionListener(String topic) {
			super();
			this.topic = topic;
		}
		
		private final String topic;
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			if(partitions.isEmpty())
				return;
			synchronized (snapshot) {
				streamToMap(partitions).forEach((k,v) -> {
					if(snapshot.containsKey(k)) {
						snapshot.get(k).removeAll(v);
					}
				});
			}
			
			if (log.isInfoEnabled()) {
				snapshot.forEach((k, v) -> {
					if (v.isEmpty()) {
						log.warn("All partitions revoked for topic - " + k);
					}
				});
				log.info("partitions revoked " + partitions);
			}
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			if(partitions.isEmpty())
				return;
			synchronized (snapshot) {
				streamToMap(partitions).forEach((k,v) -> snapshot.put(k, v));
				snapshot.notifyAll();
			}
			log.info("partitions assigned "+partitions);
		}
		/**
		 * 
		 * @param topic
		 * @return
		 */
		public Set<TopicPartition> getSnapshot() {
			synchronized (snapshot) {
				return snapshot.containsKey(topic) ? Collections.unmodifiableSet(new HashSet<>(snapshot.get(topic)))
						: Collections.emptySet();
			}
		}
		public void awaitOnReady() {
			awaitOnReady(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		}
		public boolean awaitOnReady(long duration, TimeUnit unit) {
			synchronized (snapshot) {
				if(!snapshot.containsKey(topic) || snapshot.get(topic).isEmpty()) {
					try {
						snapshot.wait(unit.toMillis(duration));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				
				return snapshot.containsKey(topic) && !snapshot.get(topic).isEmpty();
			}
		}
		
	}
	/**
	 * 
	 * @param consumerFactory
	 * @param containerProperties
	 * @param partitionListener
	 */
	public PartitionAwareMessageListenerContainer(ConsumerFactory<String, String> consumerFactory,
			ContainerProperties containerProperties, PartitionListener partitionListener) {
		super(consumerFactory, containerProperties);
		this.partitionListener = partitionListener;
	}
	public PartitionListener getPartitionListener() {
		return partitionListener;
	}
	private final PartitionListener partitionListener;
}