package org.reactiveminds.txpipe.core;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.Store;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap.ExpiringKey;
import org.reactiveminds.txpipe.utils.SelfExpiringMap.InternalMapSupplier;
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
		db = DBMaker.fileDB(ResourceUtils.getFile(fileDbPath))
				.transactionEnable()
				.fileMmapEnable()
				.make();
	}
	@PreDestroy
	private void destroy() {
		db.close();
	}
	@Override
	public LocalMapStore getObject() throws Exception {
		return getObject("default");
	}
	class MapDBSupplierProxy implements InternalMapSupplier<ExpiringKey<String>, String>, Map<ExpiringKey<String>, String>,AutoCloseable{
		private final HTreeMap<ExpiringKey<String>, String> mapFile;
		/**
		 * Load into memory.
		 */
		void load() {
			for(Store s : mapFile.getStores()) {
				s.fileLoad();
			}
		}
		public MapDBSupplierProxy(String name) {
			mapFile = db.hashMap(name, new ExpiryKeySerializer(), Serializer.STRING_ASCII).createOrOpen();
		}
		@Override
		public Map<ExpiringKey<String>, String> supply() {
			return this;
		}
		@Override
		public int size() {
			return mapFile.size();
		}
		@Override
		public boolean isEmpty() {
			return mapFile.isEmpty();
		}
		@Override
		public boolean containsKey(Object key) {
			return mapFile.containsKey(key);
		}
		@Override
		public boolean containsValue(Object value) {
			return mapFile.containsValue(value);
		}
		@Override
		public String get(Object key) {
			return mapFile.get(key);
		}
		@Override
		public String put(ExpiringKey<String> key, String value) {
			String ret = mapFile.put(key, value);
			db.commit();
			return ret;
		}
		@Override
		public String remove(Object key) {
			String ret =  mapFile.remove(key);
			db.commit();
			return ret;
		}
		@Override
		public void putAll(Map<? extends ExpiringKey<String>, ? extends String> m) {
			mapFile.putAll(m);
			db.commit();
		}
		@Override
		public void clear() {
			mapFile.clear();
			db.commit();
		}
		@Override
		public Set<ExpiringKey<String>> keySet() {
			return mapFile.keySet();
		}
		@Override
		public Collection<String> values() {
			return mapFile.values();
		}
		@Override
		public Set<Entry<ExpiringKey<String>, String>> entrySet() {
			return mapFile.entrySet();
		}
		@Override
		public void close() throws Exception {
			mapFile.close();
		}
		
	}
	/**
	 * Prototype map store.
	 * @param mapName
	 * @return
	 * @throws Exception
	 */
	public LocalMapStore getObject(String mapName) throws Exception {
		return new MapDBStore(new MapDBSupplierProxy(mapName), Long.MAX_VALUE, mapName);
	}

	@Override
	public Class<?> getObjectType() {
		return LocalMapStore.class;
	}

}
