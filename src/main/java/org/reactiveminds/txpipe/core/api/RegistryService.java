package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.PipelineDef;

public interface RegistryService {
	/**
	 * Add a new component (or replace if present) to the registry
	 * @param component
	 */
	void add(PipelineDef pipeline);
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
	PipelineDef get(String pipelineId);
	/**
	 * Get the source queue for triggering the pipeline
	 * @param componentId
	 * @return
	 */
	String getSource(String pipelineId);
	
}
