package org.reactiveminds.txpipe.spi;

import org.reactiveminds.txpipe.utils.JsonMapper;

/**
 * Record of an event, as processed by the transaction pipeline.
 * @author Sutanu_Dalui
 *
 */
public class EventRecord {

	public EventRecord(String topic, int partition, long offset, long timestamp, String key) {
		super();
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
		this.timestamp = timestamp;
		this.key = key;
	}
	public EventRecord() {
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public int getPartition() {
		return partition;
	}
	public void setPartition(int partition) {
		this.partition = partition;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public boolean isRollback() {
		return isRollback;
	}
	public void setRollback(boolean isRollback) {
		this.isRollback = isRollback;
	}
	public boolean isError() {
		return isError;
	}
	public void setError(boolean isError) {
		this.isError = isError;
	}
	public String getErrorDetail() {
		return errorDetail;
	}
	public void setErrorDetail(String errorDetail) {
		this.errorDetail = errorDetail;
	}
	private String topic;
    private int partition;
    private long offset;
    private long timestamp;
	private String value;
	private String key;
	private boolean isRollback;
	private boolean isError;
	private String errorDetail;
	@Override
	public String toString() {
		return JsonMapper.serialize(this);
	}
}
