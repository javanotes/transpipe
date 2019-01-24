package org.reactiveminds.txpipe.api;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.core.LocalMapStoreFactory;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * An abstract implementation of {@linkplain TransactionService} with no operation implementation
 * of the lifecycle methods and {@link #abort(String)} simply invoking {@link #rollback(String)}.
 * @author Sutanu_Dalui
 *
 */
public abstract class AbstractTransactionService implements TransactionService {

	@Autowired
	private LocalMapStoreFactory factory;
	
	private LocalMapStore mapstore;
	private void initMapStore() throws Exception {
		mapstore = factory.getObject(getClass().getSimpleName());
		Thread t = new Thread((Runnable)mapstore, "MapStoreWorker."+getClass().getSimpleName());
		t.setDaemon(true);
		t.start();
	}
	@Override
	public void destroy() throws Exception {
		// noop
		
	}
	@Override
	public void abort(String txnId) {
		rollback(txnId);
	}
	@Override
	public void afterPropertiesSet() throws Exception {
		initMapStore();
	}
	/**
	 * Save a string key,value to a local persistent store.
	 * @param key
	 * @param value
	 * @param ttl time to live. keys will be expired after that.
	 * @param unit
	 */
	protected void saveToStore(String key, String value, long ttl, TimeUnit unit) {
		mapstore.put(key, value, unit.toMillis(ttl));
	}
	/**
	 * Get from store
	 * @param key
	 * @return
	 */
	protected String getFromStore(String key) {
		return mapstore.get(key);
	}
	/**
	 * Check if present in store
	 * @param key
	 * @return
	 */
	protected boolean presentInStore(String key) {
		return mapstore.containsKey(key);
	}
	/**
	 * Remove from store
	 * @param key
	 */
	protected void deleteFromStore(String key) {
		mapstore.remove(key);
	}
}
