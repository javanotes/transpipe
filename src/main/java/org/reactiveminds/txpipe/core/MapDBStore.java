package org.reactiveminds.txpipe.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactiveminds.txpipe.core.LocalMapStoreFactory.MapDBSupplierProxy;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.springframework.beans.factory.InitializingBean;

/**
 * Implementation of {@linkplain LocalMapStore} that uses MapDB as backing persistence layer.
 * @author Sutanu_Dalui
 *
 */
class MapDBStore extends SelfExpiringHashMap<String, String> implements LocalMapStore,InitializingBean {

	private final MapDBSupplierProxy mapdb;
	private final String name;
	/**
	 * 
	 * @param supp
	 * @param defaultMaxLifeTimeMillis
	 * @param name
	 */
    public MapDBStore(MapDBSupplierProxy supp, long defaultMaxLifeTimeMillis, String name) {
        super(supp, defaultMaxLifeTimeMillis);
        this.name = name;
        mapdb = supp;
    }
    @Override
	public void afterPropertiesSet() {
    	mapdb.load();
		removeAllExpired();
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
			workerThread = Executors.newSingleThreadExecutor(r->{
				Thread t = new Thread(r, "MapStoreWorker."+name());
				return t;
			});
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
    
}