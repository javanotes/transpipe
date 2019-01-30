package org.reactiveminds.txpipe.utils;

import java.util.Map;
/**
 * An extension to {@linkplain Map} that supports entry expiration on configured time to live,
 * via {@link #put(Object, Object, long)} method, and registering a listener via {@link #addExpiryListener(ExpirationListener)}.
 * @author Sutanu_Dalui
 *
 * @param <K>
 * @param <V>
 */
public interface SelfExpiringMap<K, V> extends Map<K, V> {
	
	/**
	 * A supplier for custom map implementations. This can be used for instance,
	 * in having write through persistent backing maps.
	 * @author Sutanu_Dalui
	 *
	 * @param <K>
	 * @param <V>
	 */
	@FunctionalInterface
	public static interface InternalMapSupplier<K, V>{
		/**
		 * Supply a {@linkplain Map} implementation.
		 * @return
		 */
		Map<K, V> supply();
	}
	/**
	 * renew the key, may be because it has been retouched. Used internally.
	 * @param key
	 * @return
	 */
	boolean renewKey(Object key);
	/**
	 * Put key, value for a max time to live. After the time has expired, the entry will be removed 
	 * asynchronously from the map. If a listener had been added via {@link #addExpiryListener(ExpirationListener)}, it will
	 * be notified by passing the expired key.
	 * @param key
	 * @param value
	 * @param lifeTimeMillis
	 * @return
	 */
	V put(K key, V value, long lifeTimeMillis);
	/**
	 * Add an {@linkplain ExpirationListener} to listen on expired keys.
	 * @param listener
	 */
	void addExpiryListener(ONotificationListener listener);
}
