package org.reactiveminds.txpipe.spi.impl;

import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The default {@linkplain DiscoveryAgent}. Expects the service to be present in Spring context. Non Spring applications
 * can provide a different agent to load service class.
 * @author Sutanu_Dalui
 *
 */
public class DefaultDiscoveryAgent implements DiscoveryAgent{

	@Autowired
	private BeanFactory beanFactory;
	@Override
	public TransactionService getServiceById(String id) {
		return beanFactory.getBean(id, TransactionService.class);
	}
	
}