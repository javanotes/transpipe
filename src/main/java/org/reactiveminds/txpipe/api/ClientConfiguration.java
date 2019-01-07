package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.api.EventRecorder.CommitEventRecorder;
import org.reactiveminds.txpipe.api.EventRecorder.RollbackEventRecorder;
import org.reactiveminds.txpipe.core.EngineConfiguration;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The base {@linkplain Configuration} class to be imported in project starter configuration
 * class.
 * @author Sutanu_Dalui
 *
 */
@Configuration
@Import(EngineConfiguration.class)
public class ClientConfiguration {

	@ConditionalOnMissingBean(name = ComponentManager.ROLLBACK_RECORDER_BEAN_NAME)
	@Bean(ComponentManager.ROLLBACK_RECORDER_BEAN_NAME)
	EventRecorder rollbackRecorder() {
		return new RollbackEventRecorder() {
			
			@Override
			protected void onRecord(EventRecord record) {
				eventLogger.info("Rollback : " + record);
			}
		};
	}
	
	@ConditionalOnMissingBean(name = ComponentManager.COMMIT_RECORDER_BEAN_NAME)
	@Bean(ComponentManager.COMMIT_RECORDER_BEAN_NAME)
	EventRecorder commitRecorder() {
		return new CommitEventRecorder() {
			
			@Override
			protected void onRecord(EventRecord record) {
				eventLogger.info("Commit : " + record);
			}
		};
	}

}
