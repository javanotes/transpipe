package org.reactiveminds.txpipe.core.dto;

public class TransactionResult{
	public TransactionResult() {
	}
	public TransactionResult(String txnId, State status) {
		super();
		this.txnId = txnId;
		this.status = status;
	}
	public static enum State{
		COMMIT,ROLLBACK,TIMEOUT,ABORT,SUBMIT, UNDEF
	}
	//not adding any other detail of individual components
	//that should be queried from event logs
	private String txnId;
	private State status;
	public String getTxnId() {
		return txnId;
	}
	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}
	public State getStatus() {
		return status;
	}
	public void setStatus(State status) {
		this.status = status;
	}
}
