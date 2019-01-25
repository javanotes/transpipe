package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.PlatformConfiguration;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.springframework.beans.factory.BeanNameAware;

/**
 * An abstract implementation of {@linkplain TransactionService} with an optional {@linkplain LocalMapStore} instance injected
 * , a no operation {@link #destroy()} and {@link #abort(String)} simply invoking {@link #rollback(String)}. <p>It is advisable to extend
 * this class than directly implementing the {@linkplain TransactionService} interface.
 * Remember to cleanup by invoking {@linkplain LocalMapStore#destroy()}, if this is {@linkplain LocalMapStoreAware}.
 * @author Sutanu_Dalui
 *
 * @see LocalMapStoreAware
 */
public abstract class AbstractTransactionService implements TransactionService,BeanNameAware {

	@Override
	public void destroy() throws Exception {
		//
	}
	@Override
	public void abort(String txnId) {
		rollback(txnId);
	}
	@Override
	public void afterPropertiesSet() throws Exception {
		if(this instanceof LocalMapStoreAware) {
			ServiceManager factory = PlatformConfiguration.getBeanOfType(ServiceManager.class);
			((LocalMapStoreAware)this).setMapStore(factory.getMapStore(beanName));
		}
	}
	private String beanName;
	@Override
	public void setBeanName(String name) {
		beanName = name;
	}
		
}
