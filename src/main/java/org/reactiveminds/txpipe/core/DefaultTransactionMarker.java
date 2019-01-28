package org.reactiveminds.txpipe.core;

import javax.annotation.PostConstruct;

import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.dto.TransactionState;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.reactiveminds.txpipe.utils.SelfExpiringMap.ExpirationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

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
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final Logger log = LoggerFactory.getLogger("TransactionMarker");
	private SelfExpiringHashMap<String, Object> expiryCache;
	
	private final Object marker = new Object();
	@Value("${txpipe.core.abortTxnOnTimeout:true}")
	private boolean abortOnTimeout;
	@Value("${txpipe.core.abortTxnOnTimeout.expiryMillis:5000}")
	private long expiryMillis;
	
	@PostConstruct
	private void init() {
		if (abortOnTimeout) {
			expiryCache = new SelfExpiringHashMap<>(expiryMillis);
			expiryCache.addListener(this);
			Thread t = new Thread(expiryCache, "TransactionExpirationWorker");
			t.setDaemon(true);
			t.start();
		}
	}
	@Override
	public void begin(String txnId) {
		log.info("Begin : "+txnId);
		expiryCache.put(txnId, marker);
	}

	@Override
	public void end(String txnId, boolean commit) {
		log.info("End : "+txnId+", commit? "+commit);
		expiryCache.remove(txnId);
	}

	@Override
	protected void onExpiry(String key) {
		log.warn("Aborting txn on expiration : " + key);
		serviceManager.abort(key);
	}
	@Override
	public void update(TransactionState state) {
		kafkaTemplate.send(ComponentManager.TXPIPE_STATE_TOPIC, state.getTransactionId(), JsonMapper.serialize(state));
	}

}
