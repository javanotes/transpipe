package org.reactiveminds.txpipe.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class ResponsiveKafkaTemplate extends ReplyingKafkaTemplate<String, String, String> {

	/**
	 * 
	 * @param producerFactory
	 * @param replyContainer
	 */
	public ResponsiveKafkaTemplate(ProducerFactory<String, String> producerFactory,
			GenericMessageListenerContainer<String, String> replyContainer) {
		super(producerFactory, replyContainer);
	}
	protected ResponsiveKafkaTemplate(ProducerFactory<String, String> producerFactory,
			GenericMessageListenerContainer<String, String> replyContainer, Map<String, RequestReplyFuture<String, String, String>> futureMap) {
		this(producerFactory, replyContainer);
		this.futureMap = futureMap;
	}
	
	protected Map<String, RequestReplyFuture<String, String, String>> futureMap = new ConcurrentHashMap<>();
	/**
	 * Register a promise to keep 
	 * @param txnId
	 */
	final RequestReplyFuture<String, String, String> takePromise(String txnId) {
		TemplateRequestReplyFuture future = new TemplateRequestReplyFuture();
		this.futureMap.put(txnId, future);
		return future;
	}
	/**
	 * A listenable future for requests/replies.
	 *
	 * @param <K> the key type.
	 * @param <V> the outbound data type.
	 * @param <R> the reply data type.
	 *
	 */
	static class TemplateRequestReplyFuture extends RequestReplyFuture<String, String, String> {

		TemplateRequestReplyFuture() {
			super();
		}

		@Override
		protected void setSendFuture(ListenableFuture<SendResult<String, String>> sendFuture) {
			super.setSendFuture(sendFuture);
		}

	}
	@Override
	public void onMessage(List<ConsumerRecord<String, String>> data) {
		
		data.forEach(record -> {
			RequestReplyFuture<String, String, String> future = this.futureMap.remove(record.key());
			if (future == null) {
				if(this.logger.isDebugEnabled()) {
					this.logger.debug("No pending reply: " + record + " with correlationId: "+ record.key());
				}
			}
			else {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Received: " + record + " with correlationId: " + record.key());
				}
				future.set(record);
			}
		});
	}

}
