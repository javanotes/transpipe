package org.reactiveminds.txpipe.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.broker.NotifyingKafkaTemplate;
import org.reactiveminds.txpipe.broker.ResponsiveKafkaTemplate;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.dto.TransactionState;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.reactiveminds.txpipe.utils.SelfExpiringMap.ExpirationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * A default implementation of {@linkplain TransactionMarker} that rolls back transaction components
 * after a configured expiration interval. <p>Additionally, it also provides an interface to record transaction status.
 * Note, this is different from {@linkplain EventRecorder}, which is basically a complete audit log of all events, along
 * with their payload, and can be enabled optionally.
 * @author Sutanu_Dalui
 *
 */
public class DefaultTransactionMarker extends ExpirationListener<String> implements TransactionMarker {

	@Autowired
	private ServiceManager serviceManager;
	@Autowired
	BeanFactory beans;
	private ResponsiveKafkaTemplate kafkaTemplate;
	@Value("${txpipe.core.instanceId}")
	private String groupId;
	
	private static final Logger log = LoggerFactory.getLogger("TransactionMarker");
	
	//this has to be a central configuration, same for all nodes
	@Value("${txpipe.core.abortTxnOnTimeout:true}")
	private boolean abortOnTimeout;
	@Value("${txpipe.core.abortTxnOnTimeout.expiryMillis:5000}")
	private long expiryMillis;
	
	private ExecutorService workerThread;
	
	@PostConstruct
	private void init() {
		if (abortOnTimeout) {
			kafkaTemplate = beans.getBean(NotifyingKafkaTemplate.class, groupId+".ExpiryNotif", ComponentManager.TXPIPE_NOTIF_TOPIC);
			((NotifyingKafkaTemplate) kafkaTemplate).setExpiryListener(this);
			workerThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "TransactionExpiryNotifRunner"));
			workerThread.execute((NotifyingKafkaTemplate) kafkaTemplate);//starts the expired entry notifier thread
			kafkaTemplate.start();//starts the reply topic listener thread
		}
		else {
			kafkaTemplate = beans.getBean(ResponsiveKafkaTemplate.class);
		}
	}
	@Override
	public void begin(String txnId, long expiry, TimeUnit unit) {
		log.info("Begin : "+txnId);
		if (abortOnTimeout) {
			((NotifyingKafkaTemplate) kafkaTemplate).promiseNotification(txnId, expiry, unit);
		}
	}
	@Override
	public void end(String txnId, boolean commit) {
		log.info("End : "+txnId+", commit? "+commit);
		if (abortOnTimeout) {
			kafkaTemplate.send(ComponentManager.TXPIPE_NOTIF_TOPIC, txnId, commit ? TransactionResult.COMMIT.name() : TransactionResult.ROLLBACK.name());//this would remove the entry added in begin()
		}
		kafkaTemplate.send(ComponentManager.TXPIPE_REPLY_TOPIC, txnId, commit ? TransactionResult.COMMIT.name() : TransactionResult.ROLLBACK.name());
	}

	@Override
	protected void onExpiry(String key) {
		log.warn("Aborting txn on expiration : " + key);
		if (abortOnTimeout) {
			serviceManager.abort(key);
		}
	}
	@Override
	public void update(TransactionState state) {
		kafkaTemplate.send(ComponentManager.TXPIPE_STATE_TOPIC, state.getTransactionId(), JsonMapper.serialize(state));
	}

	@PreDestroy
	private void onDestroy() {
		if(workerThread != null)
			workerThread.shutdown();
	}
	
}
