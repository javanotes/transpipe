package org.reactiveminds.txpipe.core.api;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.core.Event;

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

}