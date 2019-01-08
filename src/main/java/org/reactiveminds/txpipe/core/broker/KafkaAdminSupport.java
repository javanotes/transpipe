package org.reactiveminds.txpipe.core.broker;

import java.util.Set;

interface KafkaAdminSupport {

	/**
	 * List consumers for a given topic. This operation will browse through all available consumers
	 * 
	 * @deprecated Need v2 or newer to request all topic partitions<p>
	 * @param topic
	 * @return
	 */
	Set<String> listConsumers(String topic);

}