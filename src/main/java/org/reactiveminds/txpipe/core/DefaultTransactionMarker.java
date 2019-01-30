package org.reactiveminds.txpipe.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.broker.NotifyingKafkaTemplate;
import org.reactiveminds.txpipe.broker.ResponsiveKafkaTemplate;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.dto.Event;
import org.reactiveminds.txpipe.core.dto.TransactionResult;
import org.reactiveminds.txpipe.core.dto.TransactionState;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.store.LocalMapStore;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.reactiveminds.txpipe.utils.ONotificationListener;
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
class DefaultTransactionMarker implements TransactionMarker {

	@Autowired
	private ServiceManager serviceManager;
	@Autowired
	BeanFactory beans;
	private ResponsiveKafkaTemplate kafkaTemplate;
	@Value("${txpipe.core.instanceId}")
	private String groupId;
	
	private static final Logger log = LoggerFactory.getLogger("TransactionMarker");
	
	//this has to be a central configuration, same for all nodes
	//hence disabling individual configurations
	//@Value("${txpipe.core.abortTxnOnTimeout:true}")
	private boolean abortOnTimeout = true;
	
	private ExecutorService workerThread;
	//we will not be expiring keys here
	@Autowired
	private LocalMapStore markerStore;
	
	private void deleteKey(String key) {
		markerStore.delete(key);
	}
	private void abortKey(String key) {
		log.warn("[TRIGGER_TXN_ABORT] : " + key);
		deleteKey(key);
		serviceManager.abort(key);
	}
	
	private void startReactiveWorkers() {
		ONotificationListener listener = n -> {
			switch(n.type) {
				case FUTURE_SET:
					deleteKey((String) n.object);
					break;
				case KEY_EXPIRED:
					if (abortOnTimeout) {
						abortKey((String) n.object);
					}
					break;
				default:
					break;
			}
		};
		markerStore.removeExpired(listener);
		
		((NotifyingKafkaTemplate) kafkaTemplate).setNotificationListener(listener);
		workerThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "TransactionExpiryNotifRunner"));
		workerThread.execute((NotifyingKafkaTemplate) kafkaTemplate);//starts the expired entry notifier thread
		kafkaTemplate.start();//starts the reply topic listener thread
	}
	@PostConstruct
	private void init() {
		if (abortOnTimeout) {
			kafkaTemplate = beans.getBean(NotifyingKafkaTemplate.class, groupId+".ExpiryNotif", ComponentManager.TXPIPE_NOTIF_TOPIC);
			startReactiveWorkers();
		}
		else {
			kafkaTemplate = beans.getBean(ResponsiveKafkaTemplate.class);
		}
	}
	@Override
	public void begin(Event txn, long expiry, TimeUnit unit) {
		log.info("Begin : "+txn.getTxnId());
		if (abortOnTimeout) {
			//not necessarily all-or-none
			markerStore.save(txn.getTxnId(), "-", expiry, unit);
			((NotifyingKafkaTemplate) kafkaTemplate).promiseNotification(txn.getTxnId(), expiry, unit);
		}
	}
	@Override
	public void end(String txnId, boolean commit) {
		log.info("End : " + txnId + ", commit? " + commit);
		if (abortOnTimeout) {
			// this would remove the entry added in begin(). Note, this can be invoked from a different node
			kafkaTemplate.send(ComponentManager.TXPIPE_NOTIF_TOPIC, txnId,
					commit ? TransactionResult.State.COMMIT.name() : TransactionResult.State.ROLLBACK.name());
		}
		kafkaTemplate.send(ComponentManager.TXPIPE_REPLY_TOPIC, txnId, makeReplyString(txnId, commit));
	}

	private static String makeReplyString(String txnId, boolean isCommit) {
		return isCommit ? JsonMapper.makeResponse(new TransactionResult(txnId, TransactionResult.State.COMMIT))
				: JsonMapper.makeResponse(new TransactionResult(txnId, TransactionResult.State.ROLLBACK));
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
