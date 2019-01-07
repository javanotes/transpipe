package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.core.ComponentDef;
import org.reactiveminds.txpipe.err.CommitFailedException;

/**
 * This class needs to be implemented as a core transaction service, and will be mapped to each
 * {@linkplain ComponentDef#componentId}. Declare the implementation as a Spring singleton bean in the application. Naming of the bean should be unique
 * as it will be considered as the identifier for the component during orchestration.
 * @author Sutanu_Dalui
 *
 */
public interface TransactionService {
	/**
	 * Commit this transaction request. The global txnId passed should be stored for further referencing, in case of a 
	 * future rollback triggered.
	 * @param payload json request
	 * @return json response (or null)
	 */
	String commit(String txnId, String payload) throws CommitFailedException;
	/**
	 * Rollback the request for the given txnId. Since only the txnId is passed, it is imperative that
	 * implementing services should make some provision to refer back to the original request on rollback.
	 * @param txnId
	 */
	void rollback(String txnId);
	
}
