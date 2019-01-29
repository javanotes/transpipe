package org.reactiveminds.txpipe.core.api;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.core.dto.Event;
import org.reactiveminds.txpipe.core.dto.TransactionResult;

public interface Publisher {

	
	/**
	 * To be invoked from the initial component only.
	 * @param payload
	 * @param queue
	 * @param txnId
	 * @return
	 */
	String publish(String payload, String queue, String pipeline);
	/**
	 * Execute in synchronous mode.
	 * @param payload
	 * @param queue
	 * @param pipeline
	 * @param wait
	 * @param unit
	 */
	TransactionResult execute(String payload, String queue, String pipeline, long wait, TimeUnit unit);

	/**
	 * 
	 * @param event
	 * @return
	 */
	String publish(Event event);

	/**
	 * To be invoked from initial component only.
	 * @param payload
	 * @param queue
	 * @return
	 */
	Future<?> publishAsync(String payload, String queue, String pipeline);

	/**
	 * 
	 * @param event
	 * @return
	 */
	Future<?> publishAsync(Event event);
	
	char KEY_SEP = '|';
	/**
	 * extract the pipeline from key.
	 * @param key
	 * @return
	 */
	public static String extractPipeline(String key) {
		int i = -1;
		if((i = key.indexOf(KEY_SEP)) != -1) {
			return key.substring(0,i);
		}
		throw new IllegalArgumentException("Not a valid key '"+key+"'");
	}
	public static String extractTxnId(String key) {
		int i = -1;
		if((i = key.indexOf(KEY_SEP)) != -1) {
			return key.substring(i+1);
		}
		throw new IllegalArgumentException("Not a valid key '"+key+"'");
	}
}