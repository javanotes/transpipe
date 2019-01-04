package org.reactiveminds.txpipe.core;

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
	 * @param rollbackLink
	 */
	void setRollbackLink(String rollbackLink);

}