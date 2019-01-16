package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.Event;

public interface Subscriber extends Runnable{
	/**
	 * 
	 */
	void stop();
	/**
	 * 
	 * @return
	 */
	String getListenerId();
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
	
	void pause();
	void resume();
}