package org.reactiveminds.txpipe;

import org.reactiveminds.txpipe.broker.ComponentManagerConfiguration;
import org.reactiveminds.txpipe.core.ServiceManagerConfiguration;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.spi.PayloadCodec;
import org.reactiveminds.txpipe.spi.impl.DefaultDiscoveryAgent;
import org.reactiveminds.txpipe.spi.impl.JavaScriptDiscoveryAgent;
import org.reactiveminds.txpipe.spi.impl.LogbackEventRecorder;
import org.reactiveminds.txpipe.spi.impl.SnappyPayloadCodec;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The base {@linkplain Configuration} class to be imported in project starter configuration
 * @author Sutanu_Dalui
 *
 */
@Configuration
@Import({ServiceManagerConfiguration.class, ComponentManagerConfiguration.class})
public class PlatformConfiguration implements ApplicationContextAware{
	
	@ConditionalOnProperty(name = "txpipe.event.recorder.enable")
	@ConditionalOnMissingBean
	@Bean
	EventRecorder eventRecorder() {
		return new LogbackEventRecorder();
	}
	@ConditionalOnMissingBean
	@Bean
	PayloadCodec payloadCodec() {
		return new SnappyPayloadCodec();
	}
	@Value("${txpipe.core.discoveryAgent.js:false}")
	private boolean enableJsAgent;
	
	@ConditionalOnMissingBean
	@Bean
	DiscoveryAgent discovery() {
		return enableJsAgent ? new JavaScriptDiscoveryAgent() : new DefaultDiscoveryAgent();
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
	public static <T> T getBeanOfNameAndType(String name, Class<T> requiredType) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(name, requiredType);
	}
	public static Object getBeanOfName(String name, Object...args) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(name, args);
	}
	public static <T> T getBeanOfType(Class<T> requiredType, Object...args) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return springContext.getBean(requiredType, args);
	}
	public static String getApplicationProperty(String prop) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return ((ConfigurableApplicationContext)springContext).getEnvironment().getProperty(prop);
	}
	public static String getApplicationProperty(String prop, String defaultVal) {
		if(springContext == null)
			throw new IllegalStateException("Context not initialized!");
		
		return ((ConfigurableApplicationContext)springContext).getEnvironment().getProperty(prop, defaultVal);
	}
}
