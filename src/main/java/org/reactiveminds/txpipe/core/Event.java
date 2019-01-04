package org.reactiveminds.txpipe.core;

public class Event {
	public Event() {
	}
	public String getPayload() {
		return payload;
	}
	public void setPayload(String payload) {
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
	public void setEventId(long eventId) {
		this.eventId = eventId;
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
	private String payload;
	private String txnId;
	private long eventId;
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
	protected Event(String payload, String txnId, long eventId, long timestamp, String destination) {
		super();
		this.payload = payload;
		this.txnId = txnId;
		this.eventId = eventId;
		this.timestamp = timestamp;
		this.destination = destination;
	}
	public Event copy() {
		return new Event(this.payload, this.txnId, this.eventId, this.timestamp, this.destination);
	}
	public String getPipeline() {
		return pipeline;
	}
	public void setPipeline(String pipeline) {
		this.pipeline = pipeline;
	}
}
