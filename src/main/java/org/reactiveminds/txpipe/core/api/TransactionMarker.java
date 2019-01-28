package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.dto.TransactionState;

/**
 * Interface to extend functionalities on begin/end of transactions.
 * @author Sutanu_Dalui
 *
 */
public interface TransactionMarker {
	/**
	 * Invoked when transaction starts
	 * @param txnId
	 */
	void begin(String txnId);
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
