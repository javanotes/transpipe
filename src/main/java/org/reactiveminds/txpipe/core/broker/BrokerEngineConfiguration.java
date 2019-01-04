package org.reactiveminds.txpipe.core.broker;

import org.reactiveminds.txpipe.core.ComponentManager;
import org.reactiveminds.txpipe.core.RegistryService;
import org.reactiveminds.txpipe.core.Subscriber;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

@Configuration
@Import(KafkaConfiguration.class)
public class BrokerEngineConfiguration {

	@Bean
	public RegistryService componentRegistry() {
		return new RegistryServiceImpl();
	}
	@Bean
	public KafkaPublisher publisher() {
		return new KafkaPublisher();
	}
	
	@Bean(ComponentManager.COMMIT_PROCESSOR_BEAN_NAME)
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	public Subscriber commitProcessor(String queueName) {
		return new CommitProcessor(queueName);
	}
	
	@Bean(ComponentManager.ROLLBACK_PROCESSOR_BEAN_NAME)
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	public Subscriber rollbackProcessor(String queueName) {
		return new RollbackProcessor(queueName);
	}
}
