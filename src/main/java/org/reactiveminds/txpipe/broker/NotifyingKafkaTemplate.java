package org.reactiveminds.txpipe.broker;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.reactiveminds.txpipe.utils.SelfExpiringMap.ExpirationListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.util.Assert;

public class NotifyingKafkaTemplate extends ResponsiveKafkaTemplate implements Runnable {

	private ExpirationListener<String> expiryListener;
	private final SelfExpiringHashMap<String, RequestReplyFuture<String, String, String>> expiryCache;
	/**
	 * 
	 * @param producerFactory
	 * @param replyContainer
	 */
	protected NotifyingKafkaTemplate(ProducerFactory<String, String> producerFactory,
			GenericMessageListenerContainer<String, String> replyContainer) {
		super(producerFactory, replyContainer);
		expiryCache = new SelfExpiringHashMap<>();
		this.futureMap = expiryCache;
		super.setAutoStartup(false);
	}
	/**
	 * Register a new promise to wait for this txnId to complete. The {@link #setExpiryListener(ExpirationListener)} listener will be notified, if there is an expiration.
	 * This is a reactive way of knowing if there was a reply (or not), within a finite time duration.
	 * @param messageKey
	 */
	public void promiseNotification(String messageKey, long timeout, TimeUnit unit) {
		takePromise(messageKey, timeout, unit);
	}
	private RequestReplyFuture<String, String, String> takePromise(String txnId, long timeout, TimeUnit unit) {
		TemplateRequestReplyFuture future = new TemplateRequestReplyFuture();
		expiryCache.put(txnId, future, unit.toMillis(timeout));
		return future;
	}
	@Override
	public void run() {
		Assert.notNull(expiryListener, "ExpirationListener not set");
		expiryCache.addListener(getExpiryListener());
		expiryCache.run();
	}
	public ExpirationListener<String> getExpiryListener() {
		return expiryListener;
	}
	public void setExpiryListener(ExpirationListener<String> expiryListener) {
		this.expiryListener = expiryListener;
	}
}
