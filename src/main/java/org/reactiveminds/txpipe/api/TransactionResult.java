package org.reactiveminds.txpipe.api;

public enum TransactionResult {

	COMMIT,ROLLBACK,TIMEOUT,ERROR;
	
	//not adding any other detail of individual components
	//that should be queried from event logs
	private String txnId;
	public String getTxnId() {
		return txnId;
	}
	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}
}
