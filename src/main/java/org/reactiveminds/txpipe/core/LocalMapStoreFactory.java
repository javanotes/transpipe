package org.reactiveminds.txpipe.core;

import java.io.FileNotFoundException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ResourceUtils;
/**
 * A factory of {@linkplain LocalMapStore} instances. Returns mapdb backed {@linkplain SelfExpiringHashMap}
 * implementation.
 * @author Sutanu_Dalui
 *
 */
public class LocalMapStoreFactory implements FactoryBean<LocalMapStore> {

	@Value("${txpipe.core.fileDbPath:file:./logs/db/file.db}")
	private String fileDbPath;
	private DB db;
	@PostConstruct
	private void open() throws FileNotFoundException {
		db = DBMaker.fileDB(ResourceUtils.getFile(fileDbPath)).transactionEnable().fileMmapEnable().make();
	}
	@PreDestroy
	private void destroy() {
		db.close();
	}
	/**
	 * Implementation of {@linkplain LocalMapStore} that uses MapDB as backing persistence layer.
	 * @author Sutanu_Dalui
	 *
	 */
	private static class MapDBStore extends SelfExpiringHashMap<String, String> implements LocalMapStore {

	    public MapDBStore(InternalMapSupplier<String, String> supp, long defaultMaxLifeTimeMillis) {
	        super(supp, defaultMaxLifeTimeMillis);
	    }
	}

	@Override
	public LocalMapStore getObject() throws Exception {
		return getObject("default");
	}
	/**
	 * No singleton map store.
	 * @param mapName
	 * @return
	 * @throws Exception
	 */
	public LocalMapStore getObject(String mapName) throws Exception {
		return new MapDBStore(() -> db.hashMap(mapName, Serializer.STRING_ASCII, Serializer.STRING_ASCII).createOrOpen(), Long.MAX_VALUE);
	}

	@Override
	public Class<?> getObjectType() {
		return LocalMapStore.class;
	}

}
