package org.reactiveminds.txpipe.core.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.reactiveminds.txpipe.core.PipelineDef;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

class AllDefinitionsLoader implements Runnable{

	private final List<PipelineDef> allDefinitions = new ArrayList<>();
	@Autowired
	BeanFactory beans;
	
	private JsonMapper mapper = new JsonMapper();
	private String queryTopic;
	/**
	 * 
	 * @param queryTopic
	 */
	public AllDefinitionsLoader(String queryTopic) {
		super();
		this.queryTopic = queryTopic;
	}

	@Override
	public void run() {
		KafkaTopicIterator iter = null;
		try 
		{
			iter = beans.getBean(KafkaTopicIterator.class, queryTopic);
			iter.run();
			allDefinitions.clear();
			while(iter.hasNext()) {
				List<PipelineDef> items = iter.next().stream().map(s -> mapper.toObject(s, PipelineDef.class)).collect(Collectors.toList());
				allDefinitions.addAll(items);
			}
						
		} 
		catch (Exception e) {
			RegistryServiceImpl.log.error("Loading of existing definitions failed on startup!", e);
		}
		finally {
			if (iter != null) {
				iter.close();
			}
		}
	}
		
}