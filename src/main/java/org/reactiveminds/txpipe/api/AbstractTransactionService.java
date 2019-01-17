package org.reactiveminds.txpipe.api;
/**
 * An abstract implementation of {@linkplain TransactionService} with no operation implementation
 * of the lifecycle methods and {@link #abort(String)} simply invoking {@link #rollback(String)}.
 * @author Sutanu_Dalui
 *
 */
public abstract class AbstractTransactionService implements TransactionService {

	@Override
	public void destroy() throws Exception {
		// noop
		
	}
	@Override
	public void abort(String txnId) {
		rollback(txnId);
	}
	@Override
	public void afterPropertiesSet() throws Exception {
		// noop
	}
}
