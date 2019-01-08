package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultTransactionMarker implements TransactionMarker {

	private static final Logger log = LoggerFactory.getLogger("DefaultTransactionMarker");
	@Override
	public void begin(String txnId) {
		log.info("Begin : "+txnId);
	}

	@Override
	public void end(String txnId, boolean commit) {
		log.info("End : "+txnId+", commit?"+commit);
	}

}
