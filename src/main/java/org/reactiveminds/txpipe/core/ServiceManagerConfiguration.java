package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceManagerConfiguration {

	@Bean
	RestServer server() {
		return new RestServer();
	}
	@Bean
	public ServiceManager componentManager() {
		return new DefaultServiceManager();
	}
}
