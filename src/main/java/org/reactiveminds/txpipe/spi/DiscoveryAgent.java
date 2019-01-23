package org.reactiveminds.txpipe.spi;

import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.spi.impl.JavaScriptDiscoveryAgent;
import org.reactiveminds.txpipe.spi.impl.DefaultDiscoveryAgent;
/**
 * This is a strategy class to discover {@linkplain TransactionService} from different context/source. By default
 * {@linkplain DefaultDiscoveryAgent} agent is used to get the service as a managed bean from spring context.
 * @author Sutanu_Dalui
 * @see DefaultDiscoveryAgent
 * @see JavaScriptDiscoveryAgent
 *
 */
public interface DiscoveryAgent {
	/**
	 * Get a service instance, singleton probably, with the given id. This id is the name used in pipeline
	 * definition.
	 * @param id
	 * @return
	 */
	TransactionService getServiceById(String id);

}
