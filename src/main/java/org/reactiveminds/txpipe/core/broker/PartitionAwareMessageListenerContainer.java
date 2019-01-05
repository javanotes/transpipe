package org.reactiveminds.txpipe.core.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

public final class PartitionAwareMessageListenerContainer extends ConcurrentMessageListenerContainer<String, String>{

	public static class PartitionListener implements ConsumerRebalanceListener{
		private static final Logger log = LoggerFactory.getLogger("PartitionListener");
		private final Set<TopicPartition> snapshot = new HashSet<>();
		
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			synchronized (snapshot) {
				snapshot.removeAll(partitions);
			}
			log.info("partitions revoked "+partitions);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			synchronized (snapshot) {
				snapshot.retainAll(partitions);
				snapshot.notifyAll();
			}
			log.info("partitions assigned "+partitions);
		}
		
		public Set<TopicPartition> getSnapshot(){
			synchronized (snapshot) {
				return Collections.unmodifiableSet(new HashSet<>(snapshot));
			}
		}
		public void awaitOnReady() {
			awaitOnReady(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		}
		public boolean awaitOnReady(long duration, TimeUnit unit) {
			synchronized (snapshot) {
				if(snapshot.isEmpty()) {
					try {
						snapshot.wait(unit.toMillis(duration));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				
				return !snapshot.isEmpty();
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