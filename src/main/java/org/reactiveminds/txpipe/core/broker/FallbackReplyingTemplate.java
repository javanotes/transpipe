package org.reactiveminds.txpipe.core.broker;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

class FallbackReplyingTemplate extends ReplyingKafkaTemplate<String, String, String> {

	private static final Logger log = LoggerFactory.getLogger("StrategicReplyTemplate");
	/**
	 * 
	 * @param producerFactory
	 * @param replyContainer
	 */
	public FallbackReplyingTemplate(ProducerFactory<String, String> producerFactory,
			GenericMessageListenerContainer<String, String> replyContainer) {
		super(producerFactory, replyContainer);
	}
	static final String HDR_ERR_TEXT = "Magic v1 does not support record headers";
	private volatile boolean supportHeaders = true;
	
	private RequestReplyFuture<String, String, String> doInvokeSuper(ProducerRecord<String, String> record) {
		try {
			return super.sendAndReceive(record);
		} 
		catch (Exception e) {
			Throwable coz = NestedExceptionUtils.getMostSpecificCause(e);
			if(coz instanceof IllegalArgumentException) {
				log.warn("Server does not support headers? Falling back to plan B : " + coz.getMessage());
				log.debug("", e);
				supportHeaders = false;
			}
			else
				throw e;
		}
		return null;
	}
	private static final char BASE64_SEP = ',';
	private final ConcurrentMap<String, RequestReplyFuture<String, String, String>> futures = new ConcurrentHashMap<>();
	private static String corrIdToString(CorrelationKey correlationId) {
		return Base64.getEncoder().encodeToString(correlationId.getCorrelationId());
	}
	private static String getCorrId(ConsumerRecord<String, String> record) {
		int i = record.key().indexOf(BASE64_SEP);
		Assert.isTrue(i != -1, "'correlationId' not found in consumed key");
		return record.key().substring(0, i);
	}
	private static ProducerRecord<String, String> makeCorrRecord(ProducerRecord<String, String> record, String corrId) {
		return new ProducerRecord<>(record.topic(), corrId + BASE64_SEP + record.key(), record.value());
	}
	
	@Override
	public RequestReplyFuture<String, String, String> sendAndReceive(ProducerRecord<String, String> record) {
		if(supportHeaders)
			return doInvokeSuper(record);
		
		String correlationId = corrIdToString(createCorrelationId(record));
		Assert.notNull(correlationId, "the created 'correlationId' cannot be null");
		
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Sending: " + record + " with correlationId: " + correlationId);
		}
		TemplateRequestReplyFuture<String, String, String> future = new TemplateRequestReplyFuture<>();
		this.futures.put(correlationId, future);
		try {
			future.setSendFuture(send(makeCorrRecord(record, correlationId)));
		}
		catch (Exception e) {
			this.futures.remove(correlationId);
			throw new KafkaException("Send failed", e);
		}
		
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
	public static class TemplateRequestReplyFuture<K, V, R> extends RequestReplyFuture<K, V, R> {

		TemplateRequestReplyFuture() {
			super();
		}

		@Override
		protected void setSendFuture(ListenableFuture<SendResult<K, V>> sendFuture) {
			super.setSendFuture(sendFuture);
		}

	}
	@Override
	public void onMessage(List<ConsumerRecord<String, String>> data) {
		if(supportHeaders) {
			super.onMessage(data);
			return;
		}
		data.forEach(record -> {
			String correlationId = getCorrId(record);
			if (correlationId == null) {
				this.logger.error("No correlationId found in reply: " + record
						+ " - to use request/reply semantics, the responding server must return the correlation id "
						+ " in the '" + KafkaHeaders.CORRELATION_ID + "' header");
			}
			else {
				RequestReplyFuture<String, String, String> future = this.futures.remove(correlationId);
				if (future == null) {
					this.logger.error("No pending reply: " + record + " with correlationId: "
							+ correlationId + ", perhaps timed out");
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Received: " + record + " with correlationId: " + correlationId);
					}
					future.set(record);
				}
			}
		});
	}

}
