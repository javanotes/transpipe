package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.core.CommitFailedException;
import org.reactiveminds.txpipe.core.ComponentDef;

/**
 * This class needs to be implemented as a core transaction service, and mapped to each
 * {@linkplain ComponentDef#componentId}. Then declare the implementation as a Spring singleton bean
 * @author Sutanu_Dalui
 *
 */
public interface TransactionService {
	/**
	 * Commit this transaction request
	 * @param payload json request
	 * @return json response (or null)
	 */
	String commit(String txnId, String payload) throws CommitFailedException;
	/**
	 * Rollback the request. The payload may be null.
	 * @param txnId
	 */
	void rollback(String txnId);
	
}
