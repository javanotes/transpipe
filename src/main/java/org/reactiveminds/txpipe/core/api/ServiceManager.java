package org.reactiveminds.txpipe.core.api;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactiveminds.txpipe.core.dto.CreatePipeline;
import org.reactiveminds.txpipe.core.dto.TransactionResult;
import org.reactiveminds.txpipe.store.LocalMapStore;

public interface ServiceManager {

	String ABORT_TOPIC_SUFFIX = "__abort";
	/**
	 * Invoke a new transaction pipeline. This is the service method to be invoked from REST endpoints
	 * @param requestJson
	 * @param componentId The first component to be triggered
	 * @return the transaction id
	 */
	TransactionResult invokePipeline(String requestJson, String pipelineId) throws IllegalArgumentException;
	/**
	 * Invoke a new transaction pipeline and wait for results for maxAwait time.
	 * @param requestJson
	 * @param pipelineId
	 * @param maxAwait
	 * @param unit
	 * @return 
	 * @throws TimeoutException
	 */
	TransactionResult executePipeline(String requestJson, String pipelineId, long maxAwait, TimeUnit unit) throws TimeoutException;

	String ROLLBACK_PROCESSOR_BEAN_NAME = "rollbackProcessor";
	String COMMIT_PROCESSOR_BEAN_NAME = "commitProcessor";
	/**
	 * Register and start a new transaction component
	 * @param params
	 * @param components
	 * @deprecated
	 */
	void registerPipeline(String params, String...components);
	/**
	 * Register and start a new transaction component
	 * @param request
	 */
	void registerPipeline(CreatePipeline request);
	/**
	 * 
	 * @param pipeline
	 * @param component
	 */
	void pause(String pipeline, String component);
	/**
	 * 
	 * @param pipeline
	 * @param component
	 */
	void resume(String pipeline, String component);
	/**
	 * 
	 * @param pipeline
	 * @param component
	 */
	void stop(String pipeline, String component);
	/**
	 * Abort the given transaction. Will rollback all components.
	 * @param txnId
	 */
	void abort(String txnId);
	/**
	 * Get an instance of a {@linkplain LocalMapStore} by name.
	 * @param name
	 * @return
	 */
	LocalMapStore getMapStore(String name);
}