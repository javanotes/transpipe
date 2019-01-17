package org.reactiveminds.txpipe.core.dto;

public class ResumePayload {
	public ResumePayload() {
	}
	
	public ResumePayload(String pipelineId, String componentId) {
		super();
		this.pipelineId = pipelineId;
		this.componentId = componentId;
	}

	private String pipelineId;
	private String componentId;
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
	
}
