package com.transaction.b2b;

import org.reactiveminds.txpipe.api.AbstractTransactionService;
import org.reactiveminds.txpipe.api.CommitFailedException;

public class CreditCheckingService extends AbstractTransactionService {

	@Override
	public void rollback(String txnId) {
		AppConfiguration.log.warn(txnId+" -- credit checking rolled back --");
	}
	@Override
	public void abort(String txnId) {
		AppConfiguration.log.warn(txnId+" -- credit checking aborted --");
	}
	@Override
	public String commit(String txnId, String payload) throws CommitFailedException {
		AppConfiguration.log.info(txnId+" -- credit checking success --");
		return "";
	}

	@Override
	public void destroy() throws Exception {
		super.destroy();
	}
}
