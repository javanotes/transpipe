package org.reactiveminds.txpipe.broker;

import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.core.api.Publisher;

public interface ConsumerRecordFilter extends Predicate<ConsumerRecord<String, String>>{
	
	ConsumerRecordFilter ALLOW_ALL = c -> true;
	ConsumerRecordFilter REJECT_ALL = c -> false;
	
	public static class PipelineAllowedFilter implements ConsumerRecordFilter{

		public PipelineAllowedFilter(String pipeline) {
			super();
			this.pipeline = pipeline;
		}
		private final String pipeline;
		@Override
		public boolean test(ConsumerRecord<String, String> k) {
			return pipeline.equals(Publisher.extractPipeline(k.key()));
		}
		
	}
	public static class RecordExpiredFilter implements ConsumerRecordFilter{

		public RecordExpiredFilter(long recordExpiryMillis) {
			super();
			this.recordExpiryMillis = recordExpiryMillis;
		}
		private final long recordExpiryMillis;
		@Override
		public boolean test(ConsumerRecord<String, String> k) {
			return System.currentTimeMillis() - k.timestamp() < recordExpiryMillis;
		}
		
	}
	public static class TransactionAbortedFilter implements ConsumerRecordFilter{

		public TransactionAbortedFilter(String txnId) {
			super();
			this.txnId = txnId;
		}
		private final String txnId;
		@Override
		public boolean test(ConsumerRecord<String, String> k) {
			return !txnId.equals(Publisher.extractTxnId(k.key()));
		}
		
	}
}