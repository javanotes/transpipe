package org.reactiveminds.txpipe.core;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.dto.Command;
import org.reactiveminds.txpipe.core.dto.Command.Code;
import org.reactiveminds.txpipe.core.dto.CreatePipeline;
import org.reactiveminds.txpipe.core.dto.PausePayload;
import org.reactiveminds.txpipe.core.dto.PipelineDef;
import org.reactiveminds.txpipe.core.dto.ResumePayload;
import org.reactiveminds.txpipe.core.dto.StopPayload;
import org.reactiveminds.txpipe.core.dto.TransactionResult;
import org.reactiveminds.txpipe.err.DataValidationException;
import org.reactiveminds.txpipe.err.TxPipeRuntimeException;
import org.reactiveminds.txpipe.store.LocalMapStore;
import org.reactiveminds.txpipe.store.LocalMapStoreFactory;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;

class DefaultServiceManager implements ServiceManager{

	@Autowired
	Publisher publisher;
	@Autowired
	ComponentManager registry;
	@Value("${txpipe.core.orchestrationTopic:managerTopic}") 
	private String orchestrationTopic;
	@Value("${txpipe.core.abortTxnOnTimeout.expiryMillis:5000}")
	private long expiryMillis;
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private LocalMapStoreFactory mapstoreFactory;
	
	private String getDestination(String pipelineId) {
		try 
		{
			if(!registry.contains(pipelineId))
				throw new IllegalArgumentException("Transaction pipeline '" +pipelineId+"' is not registered. If request has been already sent, please try after sometime");
			String queue = registry.getSource(pipelineId);
			Assert.hasText(queue, "Trigger topic not set for pipeline - " + pipelineId);
			
			return queue;
		} catch (IllegalArgumentException e) {
			throw new DataValidationException(e.getMessage());
		}
	}
	/**
	 * 
	 */
	@Override
	public TransactionResult invokePipeline(String requestJson, String pipelineId) throws IllegalArgumentException{
		String queue = getDestination(pipelineId);
		String id = publisher.publish(requestJson, queue, pipelineId);
		return new TransactionResult(id, TransactionResult.State.SUBMIT);
	}
		
	@Override
	public TransactionResult executePipeline(String requestJson, String pipelineId, long maxAwait, TimeUnit unit){
		String queue = getDestination(pipelineId);
		TransactionResult result = publisher.execute(requestJson, queue, pipelineId, maxAwait, unit);
		return result;
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
	@Override
	public void abort(String txnId) {
		Command c = new Command(Code.ABORT);
		c.setPayload(txnId);
		kafkaTemplate.send(orchestrationTopic+ABORT_TOPIC_SUFFIX, JsonMapper.serialize(c));
	}
	@Override
	public LocalMapStore getMapStore(String name) {
		try {
			LocalMapStore mapstore = mapstoreFactory.getObject(name);
			if (mapstore instanceof InitializingBean) {
				((InitializingBean) mapstore).afterPropertiesSet();
			}
			mapstore.start();
			return mapstore;
		} catch (Exception e) {
			throw new TxPipeRuntimeException("Unable to initialize map store - "+name, e);
		}
	}
	@Override
	public void registerPipeline(CreatePipeline create) {
		PipelineDef request = new PipelineDef();
		request.setPipelineId(create.getName());
		request.setExpiryMillis(create.getExpiry() > 0 ? create.getExpiryUnit().toMillis(create.getExpiry()) : expiryMillis);
		for(String component : create.getComponents()) {
			request.addComponent(component);
		}
		Command c = new Command(Code.START);
		c.setPayload(JsonMapper.serialize(request));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
	@Override
	public void registerPipeline(String pipeline, String... components) {
		PipelineDef request = new PipelineDef();
		request.setPipelineId(pipeline);
		request.setExpiryMillis(expiryMillis);
		for(String component : components) {
			request.addComponent(component);
		}
		Command c = new Command(Code.START);
		c.setPayload(JsonMapper.serialize(request));
		kafkaTemplate.send(orchestrationTopic, JsonMapper.serialize(c));
	}
}
