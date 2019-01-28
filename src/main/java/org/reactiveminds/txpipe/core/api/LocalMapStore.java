package org.reactiveminds.txpipe.core.api;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.DisposableBean;
/**
 * A simple string key,value store with self expiration of records. Records will be persistent locally. Do not forget
 * to {@link #destroy()} the instance on shutdown.
 * @author Sutanu_Dalui
 *
 */
public interface LocalMapStore extends DisposableBean {
	/**
	 * Name of the underlying map.
	 * @return
	 */
	String name();
	/**
	 * Starts the expiration worker thread. Either this method has to be invoked, or the instance has
	 * to be submitted as a runnable, for expiration to take place
	 */
	void start();
	/**
	 * 
	 * @param key
	 * @param value
	 * @param ttl
	 * @param unit
	 */
	void save(String key, String value, long ttl, TimeUnit unit);
	/**
	 * 
	 * @param key
	 * @return
	 */
	String get(String key);
	/**
	 * 
	 * @param key
	 * @return
	 */
	boolean isPresent(String key);
	/**
	 * 
	 * @param key
	 */
	void delete(String key);
}
