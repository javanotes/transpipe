package org.reactiveminds.txpipe.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.json.JSONException;
import org.json.JSONObject;
import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

class DefaultServiceManager implements ServiceManager{

	@Autowired
	Publisher publisher;
	@Autowired
	ComponentManager registry;
	
	private String getDestination(String pipelineId) {
		if(!registry.contains(pipelineId))
			throw new IllegalArgumentException("Transaction pipeline '" +pipelineId+"' is not registered. If request has been already sent, please try after sometime");
		String queue = registry.getSource(pipelineId);
		Assert.hasText(queue, "Trigger topic not set for pipeline - " + pipelineId);
		
		return queue;
	}
	/**
	 * 
	 */
	@Override
	public String invokePipeline(String requestJson, String pipelineId) throws IllegalArgumentException{
		String queue = getDestination(pipelineId);
		return publisher.publish(requestJson, queue, pipelineId);
	}
	@PostConstruct
	private void runOnStart() throws Exception {
		registerComponents();
	}
	private void registerComponents() {
	}
	@Override
	public void registerPipeline(PipelineDef defn) {
		registry.add(defn);
	}
	@Override
	public String executePipeline(String requestJson, String pipelineId, long maxAwait, TimeUnit unit)
			throws TimeoutException {
		String queue = getDestination(pipelineId);
		TransactionResult result = publisher.execute(requestJson, queue, pipelineId, maxAwait, unit);
		if(result == TransactionResult.TIMEOUT)
			throw new TimeoutException();
		JSONObject o = new JSONObject();
		try {
			o.put(result.getTxnId(), result.name());
		} catch (JSONException e) {
			//e.printStackTrace();
		}
		return o.toString();
	}
}
