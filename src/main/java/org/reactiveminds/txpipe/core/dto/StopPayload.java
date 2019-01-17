package org.reactiveminds.txpipe.core.dto;

public class StopPayload {

	public StopPayload() {
	}
	
	public StopPayload(String pipelineId, String componentId) {
		super();
		this.pipelineId = pipelineId;
		this.componentId = componentId;
	}

	public String getPipelineId() {
		return pipelineId;
	}
	public void setPipelineId(String pipelineId) {
		this.pipelineId = pipelineId;
	}
	public String getComponentId() {
		return componentId;
	}
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	private String pipelineId;
	private String componentId;
}
