package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.api.TransactionMarker;
import org.reactiveminds.txpipe.core.broker.BrokerEngineConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Component
@Import({BrokerEngineConfiguration.class})
public class EngineConfiguration {
	@Bean
	RestServer server() {
		return new RestServer();
	}
	@Bean
	public ServiceManager componentManager() {
		return new DefaultServiceManager();
	}
	@ConditionalOnMissingBean
	@Bean
	TransactionMarker transactionMarker() {
		return new DefaultTransactionMarker();
	}
	
}
