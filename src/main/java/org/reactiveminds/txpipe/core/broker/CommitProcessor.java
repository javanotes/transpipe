package org.reactiveminds.txpipe.core.broker;

import org.reactiveminds.txpipe.api.EventRecorder;
import org.reactiveminds.txpipe.core.Event;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
		return "CommitProcessor [commitLink=" + commitLink + ", listeningTopic=" + listeningTopic + ", componentId="
				+ componentId + ", rollbackLink=" + getRollbackLink() + "]";
	}
	@Override
	public void run() {
		super.run();
	}
	@Override
	public void consume(Event event) {
		try 
		{
			String response = process(event);
			if (StringUtils.hasText(commitLink)) {
				Event next = event.copy();
				next.setPayload(response);
				next.setDestination(commitLink);
				publisher.publish(next);
				log.debug("Passed commit to " + commitLink);
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
	@Override
	EventRecorder eventRecorder() {
		return recorder;
	}
	@Autowired
	@Qualifier(ComponentManager.COMMIT_RECORDER_BEAN_NAME)
	private EventRecorder recorder;
}
