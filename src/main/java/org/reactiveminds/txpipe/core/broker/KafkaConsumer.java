package org.reactiveminds.txpipe.core.broker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.CommitFailedException;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.JsonMapper;
import org.reactiveminds.txpipe.core.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;

abstract class KafkaConsumer implements Subscriber,AcknowledgingConsumerAwareMessageListener<String,String> {

	public static final class ContainerErrHandler implements ErrorHandler {
		private static final Logger CLOG = LoggerFactory.getLogger("ContainerErrHandler");
		@Override
		public void handle(Exception t, ConsumerRecord<?, ?> data) {
			if(t instanceof CommitFailedException) {
				CLOG.warn("Commit failure encountered on message from topic "+data.topic()+"["+data.partition()+"] at offset "+data.offset()+". Key - "+data.key());
				CLOG.debug("", t);
			}
			else {
				CLOG.error("Exception on consuming message from topic "+data.topic()+"["+data.partition()+"] at offset "+data.offset()+". Key - "+data.key(), t);
				CLOG.debug("Error in consumed message : "+data.value());
			}
		}
	}

	@Value("${txnpipe.broker.listenerConcurrency:1}")
	private int concurreny;
	private ConcurrentMessageListenerContainer<String, String> container;
	@Autowired
	BeanFactory factory;
	protected boolean isCommitMode = false;
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		container = (ConcurrentMessageListenerContainer<String, String>) factory.getBean("kafkaListenerContainer", topic, componentId, concurreny, new ContainerErrHandler());
		container.setupMessageListener(this);
		container.start();
	}
	protected final String topic;
	protected KafkaConsumer(String topic) {
		this.topic = topic;
	}
	private JsonMapper mapper = new JsonMapper();
	@Override
	public void stop() {
		container.stop();
	}

	@Override
	public String getListenerId() {
		return container != null ? container.getBeanName() : "";
	}

	protected String componentId;
	@Override
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	/**
	 * The core process method that should be executed. 
	 * @param event
	 * @return T the returning bean. It should not be {@linkplain Event} type
	 */
	String process(Event event) {
		//the bean should be there, as the starting the components will depend on it
		TransactionService service = (TransactionService) factory.getBean(componentId);
		if(isCommitMode) {
			return service.commit(event.getTxnId(), event.getPayload());
		}
		else
			service.rollback(event.getTxnId());
		
		return null;
		
	}
	@Override
	public void onMessage(ConsumerRecord<String, String> data, Acknowledgment ack,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {
		Event event = mapper.toObject(data.value(), Event.class);
		try {
			consume(event);
		} 
		finally {
			ack.acknowledge();//no message retry
		}
		
	}
}
