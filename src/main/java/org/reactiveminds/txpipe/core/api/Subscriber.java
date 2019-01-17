package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.dto.Event;

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
	boolean isPaused();
	boolean isRunning();
	void pause();
	void resume();
}