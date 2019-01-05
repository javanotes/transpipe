package org.reactiveminds.txpipe.core;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

class DefaultComponentManager implements ComponentManager{

	@Autowired
	Publisher publisher;
	@Autowired
	RegistryService registry;
	
	/**
	 * 
	 */
	@Override
	public String invokePipeline(String requestJson, String pipelineId) throws IllegalArgumentException{
		if(!registry.contains(pipelineId))
			throw new IllegalArgumentException("Transaction pipeline '" +pipelineId+"' is not registered. If request has been already sent, please try after sometime ..");
		String queue = registry.getSource(pipelineId);
		Assert.hasText(queue, "Trigger topic not set for pipeline - " + pipelineId);
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
}
