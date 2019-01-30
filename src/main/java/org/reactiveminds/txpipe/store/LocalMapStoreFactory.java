package org.reactiveminds.txpipe.store;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
/**
 * A factory of {@linkplain LocalMapStore} instances, created by a configured {@linkplain MapStoreProvider}.
 * @author Sutanu_Dalui
 * @see MapStoreProvider
 *
 */
public class LocalMapStoreFactory implements FactoryBean<LocalMapStore> {

	@Autowired
	private MapStoreProvider provider;
	
	@Override
	public LocalMapStore getObject() {
		return getObject("default");
	}
	
	/**
	 * Prototype map store.
	 * @param mapName
	 * @return
	 * @throws Exception
	 */
	public LocalMapStore getObject(String mapName) {
		return provider.get(mapName);
	}

	@Override
	public Class<?> getObjectType() {
		return LocalMapStore.class;
	}

}
