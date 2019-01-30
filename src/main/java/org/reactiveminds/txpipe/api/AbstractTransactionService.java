package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.store.LocalMapStore;
import org.springframework.beans.factory.BeanNameAware;

/**
 * An abstract implementation of {@linkplain TransactionService} with an optional {@linkplain LocalMapStore} instance injected
 * , a no operation {@link #destroy()} and {@link #abort(String)} simply invoking {@link #rollback(String)}. <p>It is advisable to extend
 * this class than directly implementing the {@linkplain TransactionService} interface.
 * @author Sutanu_Dalui
 *
 * @see LocalMapStoreAware
 */
public abstract class AbstractTransactionService implements TransactionService, BeanNameAware {

	@Override
	public void afterPropertiesSet() throws Exception {
		// noop
	}
	@Override
	public void destroy() throws Exception {
		//
	}
	@Override
	public void abort(String txnId) {
		rollback(txnId);
	}
	public String getBeanName() {
		return beanName;
	}
	private String beanName;
	@Override
	public void setBeanName(String name) {
		beanName = name;
	}
		
}
