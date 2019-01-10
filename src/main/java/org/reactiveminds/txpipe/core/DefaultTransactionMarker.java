package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A default implementation of {@linkplain TransactionMarker} that simply logs the start and end
 * of transaction to console.
 * @author Sutanu_Dalui
 *
 */
class DefaultTransactionMarker implements TransactionMarker {

	private static final Logger log = LoggerFactory.getLogger("TransactionMarker");
	@Override
	public void begin(String txnId) {
		log.info("Begin : "+txnId);
	}

	@Override
	public void end(String txnId, boolean commit) {
		log.info("End : "+txnId+", commit? "+commit);
	}

}
