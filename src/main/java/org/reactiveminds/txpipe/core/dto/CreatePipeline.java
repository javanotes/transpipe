package org.reactiveminds.txpipe.core.dto;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CreatePipeline {

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getComponents() {
		return components;
	}
	public void setComponents(List<String> components) {
		this.components = components;
	}
	public long getExpiry() {
		return expiry;
	}
	public void setExpiry(long expiry) {
		this.expiry = expiry;
	}
	public TimeUnit getExpiryUnit() {
		return expiryUnit;
	}
	public void setExpiryUnit(TimeUnit expiryUnit) {
		this.expiryUnit = expiryUnit;
	}
	private String name;
	private List<String> components;
	private long expiry = 0;
	private TimeUnit expiryUnit;
}
