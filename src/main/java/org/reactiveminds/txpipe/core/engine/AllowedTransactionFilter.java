package org.reactiveminds.txpipe.core.engine;

import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;

interface AllowedTransactionFilter extends Predicate<ConsumerRecord<String, String>>{
	
}