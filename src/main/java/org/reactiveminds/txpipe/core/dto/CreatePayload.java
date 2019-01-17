package org.reactiveminds.txpipe.core.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.util.Assert;

public class CreatePayload {

	/**
	 * Copy constructor
	 * @param pipelineId
	 * @param components
	 */
	public CreatePayload(String pipelineId, List<ComponentDef> components) {
		super();
		this.pipelineId = pipelineId;
		this.components.addAll(components.stream().map(c -> c.copy()).collect(Collectors.toList()));
	}

	public CreatePayload() {
	}
	
	public CreatePayload(String pipelineId) {
		super();
		this.pipelineId = pipelineId;
	}

	private String pipelineId;
	private final List<ComponentDef> components = new ArrayList<>();
	static final String COMMIT_SUFFIX = "__C";
	static final String ROLLBACK_SUFFIX = "__R";
	/**
	 * 
	 * @return
	 */
	public List<ComponentDef> getComponents() {
		return components.stream().map(c -> c.copy()).collect(Collectors.toList());
	}
	public String getOpeningChannel() {
		Assert.notEmpty(components, "Components have not been added yet!");
		return components.get(0).getCommitQueue();
	}
	/**
	 * Add a new component to the list. Will be maintained as a simple '2-way linked list' by connecting the upstream/downstream channels.
	 * @param txnBeanName
	 * @return
	 */
	public CreatePayload addComponent(String txnBeanName) {
		Assert.notNull(pipelineId, "pipelineId is not set");
		int i = components.size();
		ComponentDef current = new ComponentDef();
		current.setComponentId(txnBeanName);
		current.setCommitQueue(txnBeanName + COMMIT_SUFFIX);
		if(i > 0) {
			//link this with previous. rollback channel is needed only if there is a downstream
			ComponentDef previous = components.get(i-1);
			previous.setRollbackQueue(previous.getComponentId() + ROLLBACK_SUFFIX);
			previous.setCommitQueueNext(current.getCommitQueue());
			current.setRollbackQueuePrev(previous.getRollbackQueue());
		}
		components.add(current);
		return this;
	}
	
	public String getPipelineId() {
		return pipelineId;
	}
	public void setPipelineId(String pipelineId) {
		this.pipelineId = pipelineId;
	}
	@Override
	public String toString() {
		return "PipelineDef [pipelineId=" + pipelineId + ", components=" + components + "]";
	}
	
	
}
