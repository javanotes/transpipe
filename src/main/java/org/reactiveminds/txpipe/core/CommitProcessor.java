package org.reactiveminds.txpipe.core;

import java.util.concurrent.TimeUnit;

import org.reactiveminds.txpipe.core.dto.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

class CommitProcessor extends RollbackProcessor {

	private static final Logger log = LoggerFactory.getLogger("CommitProcessor");
	public String getCommitLink() {
		return commitLink;
	}
	@Override
	public void setCommitLink(String commitLink) {
		this.commitLink = commitLink;
	}
	private String commitLink;
	public CommitProcessor(String queueName) {
		super(queueName);
		isCommitMode = true;
	}
	
	@Override
	public String toString() {
		return "CommitProcessor [commitLink=" + commitLink + ", listeningTopic=" + getListeningTopic() + ", componentId="
				+ componentId + ", rollbackLink=" + getRollbackLink() + "]";
	}
	@Override
	public void run() {
		super.run();
	}
	private void propagate(Event event, String response) {
		Event next = event.copy();
		next.setPayload(codec.encode(response));
		next.setDestination(commitLink);
		publisher.publish(next);
		log.debug("Passed commit to " + commitLink);
	}
	@Override
	public void consume(Event event) {
		if(initialStep) {
			txnMarker.begin(event.getTxnId(), getTxnExpiryDuration(), TimeUnit.MILLISECONDS);
		}
		try 
		{
			String response = process(event);
			if (StringUtils.hasText(commitLink)) {
				propagate(event, response);
			}
			else {
				//this was the last component
				txnMarker.end(event.getTxnId(), true);
			}
		} catch (Exception e) {
			if (StringUtils.hasText(getRollbackLink())) {
				Event rollback = event.copy();
				rollback.setDestination(getRollbackLink());
				publisher.publish(rollback);
			}
			throw e;
		}
	}
	private boolean initialStep;
	/**
	 * 
	 */
	public void setInitialComponent() {
		initialStep = true;
	}
	
}
