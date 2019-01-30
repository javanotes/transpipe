package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.dto.Event;

public interface Processor extends Subscriber{
	/**
	 * 
	 * @param id
	 * @param event
	 */
	void consume(Event event);
	/**
	 * 
	 * @param commitLink
	 */
	void setCommitLink(String commitLink);
	/**
	 * 
	 * @param componentId
	 */
	void setComponentId(String componentId);
	/**
	 * 
	 * @param pipeline
	 */
	void setPipelineId(String pipeline);
	/**
	 * 
	 * @param rollbackLink
	 */
	void setRollbackLink(String rollbackLink);
	/**
	 * 
	 * @param event
	 */
	void abort(Event event);
}