package com.transaction.b2b;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.api.AbstractTransactionService;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.api.LocalMapStoreAware;
import org.reactiveminds.txpipe.core.api.LocalMapStore;

public class CreditCheckingService extends AbstractTransactionService implements LocalMapStoreAware {

	@Override
	public void rollback(String txnId) {
		if(store.isPresent(txnId)) {
			AppConfiguration.log.info(txnId+" will be deleted from store ");
			store.delete(txnId);
		}
		AppConfiguration.log.warn(txnId+" -- credit checking rolled back --");
	}
	
	@Override
	public String commit(String txnId, String payload) throws CommitFailedException {
		store.save(txnId, "COMMIT", 10, TimeUnit.SECONDS);
		AppConfiguration.log.info(txnId+" -- credit checking success --");
		return "";
	}

	@Override
	public void setMapStore(LocalMapStore mapStore) {
		store = mapStore;
	}
	@Override
	public void destroy() throws Exception {
		store.destroy();
		super.destroy();
	}
	private LocalMapStore store;
}
