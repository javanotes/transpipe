package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.dto.CreatePayload;

public interface ComponentManager {
	public static final String TXPIPE_REPLY_TOPIC = "__txpipe_reply";
	public static final String TXPIPE_STATE_TOPIC = "__txpipe_state";
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
