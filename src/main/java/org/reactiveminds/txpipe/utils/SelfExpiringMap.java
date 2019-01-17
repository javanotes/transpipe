package org.reactiveminds.txpipe.utils;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;

public interface SelfExpiringMap<K, V> extends Map<K, V> {

	boolean renewKey(Object key);
	V put(K key, V value, long lifeTimeMillis);
	void addListener(ExpirationListener<K> listener);
	/**
	 * 
	 * @author Sutanu_Dalui
	 *
	 * @param <K>
	 */
	public abstract class ExpirationListener<K> implements Observer{
		/**
		 * Callback method that will be invoked on key expiration.
		 * @param key
		 */
		protected abstract void onExpiry(K key);
		@SuppressWarnings("unchecked")
		@Override
		public void update(Observable o, Object arg) {
			onExpiry((K) arg);
		}
		
	}

}
