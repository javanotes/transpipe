package org.reactiveminds.txpipe.core.broker;

import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

class RollbackProcessor extends KafkaConsumer {

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
	
	@Override
	public void consume(Event event) {
		try {
			process(event);
		} catch (Exception e) {
			throw e;
		}
		finally {
			if (StringUtils.hasText(rollbackLink)) {
				Event rollback = event.copy();
				rollback.setDestination(rollbackLink);
				publisher.publish(rollback);
			}
		}
		
	}
	@Override
	public void setCommitLink(String commitLink) {
		//noops
	}

}
