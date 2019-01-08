package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.core.PipelineDef;

public interface ServiceManager {

	/**
	 * Start a new transaction component
	 * @param defn
	 */
	void registerPipeline(PipelineDef defn);

	/**
	 * Invoke a new transaction pipeline. This is the service method to be invoked from REST endpoints
	 * @param requestJson
	 * @param componentId The first component to be triggered
	 * @return the transaction id
	 */
	String invokePipeline(String requestJson, String pipelineId) throws IllegalArgumentException;

	String ROLLBACK_PROCESSOR_BEAN_NAME = "rollbackProcessor";
	String COMMIT_PROCESSOR_BEAN_NAME = "commitProcessor";
	String ROLLBACK_RECORDER_BEAN_NAME = "rollbackRecorder";
	String COMMIT_RECORDER_BEAN_NAME = "commitRecorder";
}