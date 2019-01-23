package org.reactiveminds.txpipe.core.dto;

public class Event {
	public Event() {
	}
	public byte[] getPayload() {
		return payload;
	}
	public void setPayload(byte[] payload) {
		this.payload = payload;
	}
	public String getTxnId() {
		return txnId;
	}
	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}
	public long getEventId() {
		return eventId;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	private byte[] payload;
	private String txnId;
	private int eventId;
	private long timestamp;
	private String destination;
	private String pipeline;
	/**
	 * Copy constructor
	 * @param payload
	 * @param txnId
	 * @param eventId
	 * @param timestamp
	 * @param destination
	 */
	private Event(byte[] payload, String txnId, int eventId, long timestamp, String destination, String pipeline) {
		super();
		this.payload = payload;
		this.txnId = txnId;
		this.eventId = eventId;
		this.timestamp = timestamp;
		this.destination = destination;
		this.pipeline = pipeline;
	}
	public Event copy() {
		return new Event(this.payload, this.txnId, this.eventId+1, this.timestamp, this.destination, this.pipeline);
	}
	public String getPipeline() {
		return pipeline;
	}
	public void setPipeline(String pipeline) {
		this.pipeline = pipeline;
	}
	@Override
	public String toString() {
		return "Event [payload=" + payload + ", txnId=" + txnId + ", eventId=" + eventId + ", timestamp=" + timestamp
				+ ", destination=" + destination + ", pipeline=" + pipeline + "]";
	}
}
