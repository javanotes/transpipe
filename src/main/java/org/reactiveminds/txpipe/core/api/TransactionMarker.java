package org.reactiveminds.txpipe.core.api;

public interface TransactionMarker {

	void begin(String txnId);
	void end(String txnId, boolean commit);
}
