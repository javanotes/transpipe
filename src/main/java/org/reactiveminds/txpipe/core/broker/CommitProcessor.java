package org.reactiveminds.txpipe.core.broker;

import org.reactiveminds.txpipe.core.Event;
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
		return " [commitLink=" + getCommitLink() + ", componentId=" + componentId + ", rollbackLink="
				+ getRollbackLink() + "]";
	}
	@Override
	public void run() {
		super.run();
		log.info("Started consumer container - "+this);
	}
	@Override
	public void consume(Event event) {
		try 
		{
			String response = process(event);
			if (StringUtils.hasText(commitLink)) {
				publisher.publish(response, commitLink, event.getPipeline(), event.getTxnId());
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
}
