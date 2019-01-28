package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.broker.KafkaSubscriber;
import org.reactiveminds.txpipe.core.api.Publisher;
import org.reactiveminds.txpipe.core.dto.Event;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

class RollbackProcessor extends KafkaSubscriber {
	
	@Override
	public String toString() {
		return "RollbackProcessor [rollbackLink=" + rollbackLink + ", listeningTopic=" + getListeningTopic()
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
		} 
		finally {
			if (StringUtils.hasText(rollbackLink)) {
				propagate(event);
			}
			else {
				//first component reached
				txnMarker.end(event.getTxnId(), false);
			}
		}
		
	}
	@Override
	public void setCommitLink(String commitLink) {
		//noops
	}
	
}
