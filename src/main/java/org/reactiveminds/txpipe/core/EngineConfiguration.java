package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.broker.BrokerEngineConfiguration;
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
	public ComponentManager componentManager() {
		return new DefaultComponentManager();
	}
	
	
}
