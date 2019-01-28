package org.reactiveminds.txpipe.broker;

import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface AllowedTransactionFilter extends Predicate<ConsumerRecord<String, String>>{
	
}