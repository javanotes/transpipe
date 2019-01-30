package org.reactiveminds.txpipe.core.dto;

import org.reactiveminds.txpipe.core.dto.TransactionResult.State;

public class TransactionState {
	public TransactionState() {
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public String getPipeline() {
		return pipeline;
	}

	public void setPipeline(String pipeline) {
		this.pipeline = pipeline;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public short getSequence() {
		return sequence;
	}

	public void setSequence(short sequence) {
		this.sequence = sequence;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	private String transactionId;
	private String component;
	private String pipeline;
	private State state;
	private long timestamp;
	private short sequence;
}
