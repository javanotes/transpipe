package org.reactiveminds.txpipe.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.json.JSONException;
import org.json.JSONObject;
import org.reactiveminds.txpipe.api.TransactionResult;
import org.reactiveminds.txpipe.core.Command.Code;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.command.CreatePayload;
import org.reactiveminds.txpipe.core.command.PausePayload;
import org.reactiveminds.txpipe.core.command.ResumePayload;
import org.reactiveminds.txpipe.core.command.StopPayload;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;

class DefaultServiceManager implements ServiceManager{

	@Autowired
	Publisher publisher;
	@Autowired
	ComponentManager registry;
	@Value("${txpipe.broker.orchestrationTopic:managerTopic}") 
	private String orchestrationTopic;
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
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
	@Override
	public void registerPipeline(String pipeline, String... components) {
		CreatePayload request = new CreatePayload();
		request.setPipelineId(pipeline);
		for(String component : components) {
			request.addComponent(component);
		}
		Command c = new Command(Code.START);
		c.setPayload(JsonMapper.serialize(request));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
	
	@Override
	public void pause(String pipeline, String component) {
		Command c = new Command(Code.PAUSE);
		c.setPayload(JsonMapper.serialize(new PausePayload(pipeline, component)));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
	@Override
	public void resume(String pipeline, String component) {
		Command c = new Command(Code.RESUME);
		c.setPayload(JsonMapper.serialize(new ResumePayload(pipeline, component)));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
	@Override
	public void stop(String pipeline, String component) {
		Command c = new Command(Code.STOP);
		c.setPayload(JsonMapper.serialize(new StopPayload(pipeline, component)));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
}
