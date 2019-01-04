package org.reactiveminds.txpipe.core;

public class ComponentDef {

	@Override
	public String toString() {
		return "[componentId=" + componentId + ", commitQueue=" + commitQueue + ", rollbackQueue="
				+ rollbackQueue + ", commitQueueNext=" + commitQueueNext + ", rollbackQueuePrev=" + rollbackQueuePrev
				+ "]";
	}
	public ComponentDef copy() {
		return new ComponentDef(componentId, commitQueue, rollbackQueue, commitQueueNext, rollbackQueuePrev);
	}
	private ComponentDef(String componentId, String commitQueue, String rollbackQueue, String commitQueueNext,
			String rollbackQueuePrev) {
		super();
		this.componentId = componentId;
		this.commitQueue = commitQueue;
		this.rollbackQueue = rollbackQueue;
		this.commitQueueNext = commitQueueNext;
		this.rollbackQueuePrev = rollbackQueuePrev;
	}
	public ComponentDef() {
	}
	public String getComponentId() {
		return componentId;
	}
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	public String getCommitQueue() {
		return commitQueue;
	}
	public void setCommitQueue(String commitQueue) {
		this.commitQueue = commitQueue;
	}
	public String getRollbackQueue() {
		return rollbackQueue;
	}
	public void setRollbackQueue(String rollbackQueue) {
		this.rollbackQueue = rollbackQueue;
	}
	public String getCommitQueueNext() {
		return commitQueueNext;
	}
	public void setCommitQueueNext(String commitQueueNext) {
		this.commitQueueNext = commitQueueNext;
	}
	public String getRollbackQueuePrev() {
		return rollbackQueuePrev;
	}
	public void setRollbackQueuePrev(String rollbackQueuePrev) {
		this.rollbackQueuePrev = rollbackQueuePrev;
	}
	/*
	 * Name of the transaction service
	 */
	private String componentId;
	/*
	 * Upstream commit queue to listen
	 */
	private String commitQueue;
	/*
	 * Downstream rollback queue for trigger
	 */
	private String rollbackQueue;
	/*
	 * Downstream commit queue to trigger
	 */
	private String commitQueueNext;
	/*
	 * Upstream rollback queue to trigger
	 */
	private String rollbackQueuePrev;
}
