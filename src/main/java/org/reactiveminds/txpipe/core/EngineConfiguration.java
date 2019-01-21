package org.reactiveminds.txpipe.core;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.engine.BrokerEngineConfiguration;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.spi.TransactionMarker;
import org.reactiveminds.txpipe.spi.impl.LogbackEventRecorder;
import org.reactiveminds.txpipe.spi.impl.TransactionExpirationMarker;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * The base {@linkplain Configuration} class to be imported in project starter configuration
 * @author Sutanu_Dalui
 *
 */
@Component
@Import({BrokerEngineConfiguration.class})
public class EngineConfiguration implements ApplicationContextAware{
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
		return new TransactionExpirationMarker();
	}
	@ConditionalOnMissingBean
	@Bean
	EventRecorder eventRecorder() {
		return new LogbackEventRecorder();
	}
	private static ApplicationContext springContext;
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		springContext = applicationContext;
	}
	/**
	 * 
	 * @param name
	 * @param requiredType
	 * @return
	 */
	public static <T> T getMangedBeanOfName(String name, Class<T> requiredType) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(name, requiredType);
	}
	public static Object getMangedBeanOfName(String name) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(name);
	}
	public static <T> T getMangedBeanOfType(Class<T> requiredType) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(requiredType);
	}
	
}
