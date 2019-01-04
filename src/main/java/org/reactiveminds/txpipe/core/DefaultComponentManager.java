package org.reactiveminds.txpipe.core;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

class DefaultComponentManager implements ComponentManager{

	private static final Logger log = LoggerFactory.getLogger(DefaultComponentManager.class);
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
		return publisher.publish(requestJson, queue, pipelineId, null);
	}
	@PostConstruct
	private void runOnStart() throws Exception {
		registerComponents();
	}
	private void registerComponents() {
		log.warn("TODO : Load component/s from configurations (?)");
	}
	@Override
	public void registerPipeline(PipelineDef defn) {
		registry.add(defn);
	}
}
