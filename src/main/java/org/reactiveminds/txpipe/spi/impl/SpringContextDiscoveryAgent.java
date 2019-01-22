package org.reactiveminds.txpipe.spi.impl;

import org.reactiveminds.txpipe.PlatformConfiguration;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;

/**
 * The default {@linkplain DiscoveryAgent}. Expects the service to be present in Spring context. Non Spring applications
 * can provide a different agent to load service class.
 * @author Sutanu_Dalui
 *
 */
public class SpringContextDiscoveryAgent implements DiscoveryAgent{

	@Override
	public TransactionService getServiceById(String id) {
		return PlatformConfiguration.getMangedBeanOfName(id, TransactionService.class);
	}
	
}