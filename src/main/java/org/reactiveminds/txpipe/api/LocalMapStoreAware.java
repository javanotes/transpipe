package org.reactiveminds.txpipe.api;

import org.reactiveminds.txpipe.core.api.LocalMapStore;
/**
 * Interface to be implemented by a {@linkplain TransactionService} implementation,
 * if it intends to use a locally persistent key value store.
 * @author Sutanu_Dalui
 *
 */
public interface LocalMapStoreAware {
	/**
	 * Set the passed instance and use it for map store operations. Note the map store
	 * will be per the bean name. Implies, the underlying fileMap will be same, for same named beans! So better to use this in singleton beans, to have 
	 * a consistent expectation.
	 * @param mapStore
	 */
	void setMapStore(LocalMapStore mapStore);
}
