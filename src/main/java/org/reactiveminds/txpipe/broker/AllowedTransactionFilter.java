package org.reactiveminds.txpipe.broker;

import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;

interface AllowedTransactionFilter extends Predicate<ConsumerRecord<String, String>>{
	
}