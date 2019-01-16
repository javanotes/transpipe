package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.command.CreatePayload;

public interface ComponentManager {
	/**
	 * If the component already exists
	 * @param componentId
	 * @return
	 */
	boolean contains(String pipelineId);
	/**
	 * Gets the corresponding component
	 * @param componentId
	 * @return
	 */
	CreatePayload get(String pipelineId);
	/**
	 * Get the source queue for triggering the pipeline
	 * @param componentId
	 * @return
	 */
	String getSource(String pipelineId);
	
}
