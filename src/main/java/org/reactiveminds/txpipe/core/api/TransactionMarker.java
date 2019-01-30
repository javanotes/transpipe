package org.reactiveminds.txpipe.core.api;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.core.dto.Event;
import org.reactiveminds.txpipe.core.dto.TransactionState;

/**
 * Interface to extend functionalities on begin/end of transactions.
 * @author Sutanu_Dalui
 *
 */
public interface TransactionMarker {
	/**
	 * Invoked when transaction starts
	 * @param txn
	 * @param expiry
	 * @param unit
	 */
	void begin(Event txn, long expiry, TimeUnit unit);
	/**
	 * Invoked when transaction ends
	 * @param txnId
	 * @param commit
	 */
	void end(String txnId, boolean commit);
	/**
	 * Update the transaction state progression.
	 * @param state
	 */
	void update(TransactionState state);
}
