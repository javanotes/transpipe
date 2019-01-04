package org.reactiveminds.txpipe.core;

import java.util.concurrent.Future;

public interface Publisher {

	
	/**
	 * To be invoked from the initial component only.
	 * @param payload
	 * @param queue
	 * @param txnId
	 * @return
	 */
	<T> String publish(String payload, String queue, String pipeline);

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