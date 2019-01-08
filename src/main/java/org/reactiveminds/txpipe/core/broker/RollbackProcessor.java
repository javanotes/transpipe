package org.reactiveminds.txpipe.core.broker;

import org.reactiveminds.txpipe.api.EventRecorder;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.StringUtils;

class RollbackProcessor extends KafkaSubscriber {
	
	@Override
	public String toString() {
		return "RollbackProcessor [rollbackLink=" + rollbackLink + ", listeningTopic=" + listeningTopic
				+ ", componentId=" + componentId + "]";
	}
	@Autowired
	Publisher publisher;
	public String getRollbackLink() {
		return rollbackLink;
	}
	@Override
	public void setRollbackLink(String rollbackLink) {
		this.rollbackLink = rollbackLink;
	}
	private String rollbackLink;
	
	public RollbackProcessor(String queueName) {
		super(queueName);
	}
	private void propagate(Event event) {
		Event rollback = event.copy();
		rollback.setDestination(rollbackLink);
		publisher.publish(rollback);
	}
	@Override
	public void consume(Event event) {
		try {
			process(event);
		} catch (Exception e) {
			throw e;
		}
		finally {
			if (StringUtils.hasText(rollbackLink)) {
				propagate(event);
			}
			else {
				//first component reached
				endTxn(event.getTxnId(), false);
			}
		}
		
	}
	@Override
	public void setCommitLink(String commitLink) {
		//noops
	}
	@Override
	EventRecorder eventRecorder() {
		return recorder;
	}
	@Autowired
	@Qualifier(ServiceManager.ROLLBACK_RECORDER_BEAN_NAME)
	private EventRecorder recorder;
}
