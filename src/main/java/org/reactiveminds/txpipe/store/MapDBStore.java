package org.reactiveminds.txpipe.store;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.Store;
import org.reactiveminds.txpipe.utils.ONotificationListener;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.springframework.beans.factory.InitializingBean;

/**
 * Implementation of {@linkplain LocalMapStore} that uses MapDB as backing persistence layer. The implementation
 * extends {@linkplain SelfExpiringHashMap}, hence entries can be expired automatically if configured.
 * @author Sutanu_Dalui
 *
 */
class MapDBStore extends SelfExpiringHashMap<String, String> implements LocalMapStore,InitializingBean {

	private static class MapDBSupplierProxy implements InternalMapSupplier<ExpiringKey<String>, String>, Map<ExpiringKey<String>, String>,AutoCloseable{
		private final HTreeMap<ExpiringKey<String>, String> mapFile;
		/**
		 * Load into memory.
		 */
		private void load() {
			for(Store s : mapFile.getStores()) {
				s.fileLoad();
			}
		}
		private final DB db;
		/**
		 * Constructor
		 * @param name
		 * @param db
		 */
		private MapDBSupplierProxy(String name, DB db) {
			this.db = db;
			mapFile = this.db.hashMap(name, new ExpiryKeySerializer(), Serializer.STRING_ASCII).createOrOpen();
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
	private final MapDBSupplierProxy mapdb;
	private final String name;
	/**
	 * 
	 * @param db
	 * @param defaultMaxLifeTimeMillis
	 * @param name
	 */
    public MapDBStore(DB db, long defaultMaxLifeTimeMillis, String name) {
        super(defaultMaxLifeTimeMillis);
        mapdb = new MapDBSupplierProxy(name, db);
        this.name = name;
        internalMap = mapdb.supply();
    }
    @Override
	public void afterPropertiesSet() {
    	//noop
	}

	@Override
	public String name() {
		return name;
	}
	private ExecutorService workerThread;
	private AtomicBoolean started = new AtomicBoolean();
	@Override
	public void start() {
		if(started.compareAndSet(false, true)) {
			workerThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "MapStoreWorker."+name()));
			workerThread.execute(this);
		}
	}

	@Override
	public void destroy() throws Exception {
		if(workerThread != null) {
			workerThread.shutdown();
			workerThread.awaitTermination(10, TimeUnit.SECONDS);
		}
		mapdb.close();
	}

	@Override
	public void save(String key, String value, long ttl, TimeUnit unit) {
		put(key, value, unit.toMillis(ttl));
	}

	@Override
	public String get(String key) {
		return super.get(key);
	}

	@Override
	public boolean isPresent(String key) {
		return containsKey(key);
	}

	@Override
	public void delete(String key) {
		remove(key);
	}
	@Override
	public void save(String key, String value) {
		put(key, value);
	}
	@Override
	public void removeExpired(ONotificationListener listener) {
		mapdb.load();
		removeAllExpired(listener);
	}
}