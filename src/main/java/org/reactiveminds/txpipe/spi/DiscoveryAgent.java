package org.reactiveminds.txpipe.spi;

import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.core.EngineConfiguration;

public interface DiscoveryAgent {
	/**
	 * Get a service instance, singleton probably, with given id. This id is the name used in pipeline
	 * definition.
	 * @param id
	 * @return
	 */
	TransactionService getServiceById(String id);
	/**
	 * The default {@linkplain DiscoveryAgent}. Expects the service to be present in Spring context. Non Spring applications
	 * can provide a different agent to load service class.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class SpringContextDiscoveryAgent implements DiscoveryAgent{

		@Override
		public TransactionService getServiceById(String id) {
			return EngineConfiguration.getMangedBeanOfName(id, TransactionService.class);
		}
		
	}
}
