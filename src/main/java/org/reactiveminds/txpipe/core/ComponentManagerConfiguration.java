package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.broker.KafkaConfiguration;
import org.reactiveminds.txpipe.broker.Subscriber;
import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

@Configuration
@Import(KafkaConfiguration.class)
public class ComponentManagerConfiguration {

	@Value("${txpipe.core.orchestrationTopic:managerTopic}") 
	private String orchestrationTopic;
	@Bean
	TransactionMarker transactionMarker() {
		return new DefaultTransactionMarker();
	}
	@Bean
	public ComponentManager componentRegistry() {
		return new DefaultComponentManager(orchestrationTopic);
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
