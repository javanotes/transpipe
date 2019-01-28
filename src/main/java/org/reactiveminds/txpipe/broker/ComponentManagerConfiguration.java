package org.reactiveminds.txpipe.broker;

import org.reactiveminds.txpipe.core.DefaultTransactionMarker;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.Subscriber;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

@Configuration
@Import(KafkaConfiguration.class)
public class ComponentManagerConfiguration {

	@Bean
	TransactionMarker transactionMarker() {
		return new DefaultTransactionMarker();
	}
	@Bean
	public ComponentManager componentRegistry() {
		return new DefaultComponentManager();
	}
	@Bean
	public KafkaPublisher publisher() {
		return new KafkaPublisher();
	}
	
	@Bean(ServiceManager.COMMIT_PROCESSOR_BEAN_NAME)
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	public Subscriber commitProcessor(String queueName) {
		return new CommitProcessor(queueName);
	}
	
	@Bean(ServiceManager.ROLLBACK_PROCESSOR_BEAN_NAME)
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	public Subscriber rollbackProcessor(String queueName) {
		return new RollbackProcessor(queueName);
	}
	
}
