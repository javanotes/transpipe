package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.core.dto.ComponentDef;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * This class needs to be implemented as a core transaction service, and will be mapped to each
 * {@linkplain ComponentDef#componentId}. Declare the implementation as a Spring singleton bean in the application. Naming of the bean should be unique
 * as it will be considered as the identifier for the component during transaction orchestration.
 * @author Sutanu_Dalui
 * @see AbstractTransactionService
 */
public interface TransactionService extends DisposableBean, InitializingBean{
	/**
	 * Commit this transaction request. The global txnId passed should be stored for further referencing, in case of a 
	 * future rollback triggered.
	 * @param payload json request
	 * @return json response (or null)
	 */
	String commit(String txnId, String payload) throws CommitFailedException;
	/**
	 * Rollback the request for the given txnId. Since only the txnId is passed, it is imperative that
	 * implementing services should make some provision to refer back to the original request on rollback. This invocation
	 * will be triggered on a downstream failure, in a cascading manner.
	 * @param txnId
	 */
	void rollback(String txnId);
	/**
	 * Force rollback the request on an orchestration process timeout. This
	 * method is invoked as a broadcast command on all participating components at the same time. In its simplest form,
	 * this method may simply call {@link #rollback(String)}.
	 * @param txnId
	 */
	void abort(String txnId);
}
