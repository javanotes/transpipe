package org.reactiveminds.txpipe.core;

import java.util.UUID;
import java.util.concurrent.Future;

import org.springframework.util.StringUtils;

import com.datastax.driver.core.utils.UUIDs;

public interface Publisher {

	static Event makeEvent(String message, String queue, String txnId) {
		Event e = new Event();
		e.setDestination(queue);
		e.setPayload(message);
		e.setTxnId(StringUtils.hasText(txnId) ? txnId : UUIDs.timeBased().toString());
		e.setEventId(UUID.fromString(e.getTxnId()).timestamp());
		e.setTimestamp(UUIDs.unixTimestamp(UUID.fromString(e.getTxnId())));
		return e;
	}
	
	/**
	 * 
	 * @param message
	 * @param queue
	 * @param txnId
	 * @return
	 */
	<T> String publish(String message, String queue, String pipeline, String txnId);

	/**
	 * 
	 * @param event
	 * @return
	 */
	String publish(Event event);

	/**
	 * 
	 * @param message
	 * @param queue
	 * @return
	 */
	Future<?> publishAsync(String message, String queue, String pipeline, String txnId);

	/**
	 * 
	 * @param event
	 * @return
	 */
	Future<?> publishAsync(Event event);

}