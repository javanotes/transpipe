package org.reactiveminds.txpipe.core.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.reactiveminds.txpipe.core.api.Subscriber;
import org.reactiveminds.txpipe.core.command.PausePayload;
import org.reactiveminds.txpipe.core.command.ResumePayload;
import org.reactiveminds.txpipe.core.command.StopPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

class ProcessorRegistry {
	private final Map<String, RegisteredProcessor> processorRegistry = Collections.synchronizedMap(new HashMap<>());
	private static final Logger log = LoggerFactory.getLogger("ProcessorRegistry");
	private ProcessorRegistry() {
	}
	private static class Loader{
		private static final ProcessorRegistry me = new ProcessorRegistry();
	}
	private static boolean isSubscriberOfPipeline(Subscriber commit, String pipe) {
		return StringUtils.hasText(commit.getListenerId()) && commit.getListenerId().startsWith(pipe);
	}
	private static boolean isSubscriberOfTransaction(Subscriber commit, String pipe, String txn) {
		return isSubscriberOfPipeline(commit, pipe) && commit.getListenerId().endsWith(txn);
	}
	public static ProcessorRegistry instance() {
		return Loader.me;
	}
	/**
	 * 
	 * @param key
	 * @param commitSub
	 * @param rollbackSub
	 */
	public void put(String key, Subscriber commitSub, Subscriber rollbackSub) {
		Assert.isInstanceOf(CommitProcessor.class, commitSub);
		if (rollbackSub != null) {
			Assert.isInstanceOf(RollbackProcessor.class, rollbackSub);
		}
		processorRegistry.put(key, new RegisteredProcessor((CommitProcessor)commitSub, (RollbackProcessor)rollbackSub));
	}
	
	/**
	 * 
	 * @param key
	 */
	public void removeIfPresent(String key) {
		if(processorRegistry.containsKey(key)) {
			log.warn("Running processors for ["+ key + "] are being stopped on new configuration received ..");
			RegisteredProcessor proc = processorRegistry.remove(key);
			proc.stop();
		}
	}
	public void pause(String pipeline) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfPipeline(e.getValue().commit, pipeline))
			.forEach(e -> e.getValue().commit.pause());
		}
	}
	public void resume(String pipeline) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfPipeline(e.getValue().commit, pipeline))
			.forEach(e -> e.getValue().commit.resume());
		}
	}
	public void stop(String pipeline) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfPipeline(e.getValue().commit, pipeline))
			.forEach(e -> e.getValue().stop());
		}
	}
	public void pause(String pipeline, String txn) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfTransaction(e.getValue().commit, pipeline, txn))
			.forEach(e -> e.getValue().commit.pause());
		}
	}
	public void resume(String pipeline, String txn) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfTransaction(e.getValue().commit, pipeline, txn))
			.forEach(e -> e.getValue().commit.resume());
		}
	}
	public void stop(String pipeline, String txn) {
		synchronized (processorRegistry) {
			processorRegistry.entrySet().stream()
			.filter(e -> isSubscriberOfTransaction(e.getValue().commit, pipeline, txn))
			.forEach(e -> e.getValue().stop());
		}
	}
	/**
	 * Pauses a running commit subscriber.
	 * @param c
	 */
	public void pause(PausePayload c) {
		if(StringUtils.hasText(c.getPipelineId())) {
			if(StringUtils.hasText(c.getComponentId()))
				pause(c.getPipelineId(), c.getComponentId());
			else
				pause(c.getPipelineId());
		}
	}
	/**
	 * Resumes the paused commit subscriber.
	 * @param c
	 */
	public void resume(ResumePayload c) {
		if(StringUtils.hasText(c.getPipelineId())) {
			if(StringUtils.hasText(c.getComponentId()))
				resume(c.getPipelineId(), c.getComponentId());
			else
				resume(c.getPipelineId());
		}
	}
	/**
	 * Stops both the commit and rollback subscriber. This is irrecoverable. Starting the transactions
	 * again will require restarting the node.
	 * @param c
	 */
	public void stop(StopPayload c) {
		if(StringUtils.hasText(c.getPipelineId())) {
			if(StringUtils.hasText(c.getComponentId()))
				stop(c.getPipelineId(), c.getComponentId());
			else
				stop(c.getPipelineId());
		}
	}
	/**
	 * 
	 */
	public void destroy() {
		synchronized (processorRegistry) {
			processorRegistry.forEach((k,v) -> v.stop());
			processorRegistry.clear();
		}
	}
	private static class RegisteredProcessor{
		private final CommitProcessor commit;
		private final RollbackProcessor rollback;
		/**
		 * 
		 * @param commit
		 * @param rollback
		 */
		public RegisteredProcessor(CommitProcessor commit, RollbackProcessor rollback) {
			super();
			this.commit = commit;
			this.rollback = rollback;
		}
		
		private void stop() {
			if(commit != null)
				commit.stop();
			if(rollback != null)
				rollback.stop();
		}
	}
	
}