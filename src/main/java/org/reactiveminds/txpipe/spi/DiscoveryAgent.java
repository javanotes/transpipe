package org.reactiveminds.txpipe.spi;

import org.reactiveminds.txpipe.api.TransactionService;

public interface DiscoveryAgent {
	/**
	 * Get a service instance, singleton probably, with given id. This id is the name used in pipeline
	 * definition.
	 * @param id
	 * @return
	 */
	TransactionService getServiceById(String id);
}
