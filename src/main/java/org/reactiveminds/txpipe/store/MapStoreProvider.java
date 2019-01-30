package org.reactiveminds.txpipe.store;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ResourceUtils;
/**
 * A provider for local key,value persistent store. By default uses {@linkplain MapDBStore}
 * @author Sutanu_Dalui
 *
 */
public interface MapStoreProvider extends InitializingBean,DisposableBean{
	/**
	 * Get a store given by name.
	 * @param name
	 * @return
	 */
	LocalMapStore get(String name);
	
	public static class RocksDBProvider implements MapStoreProvider{

		@Override
		public void afterPropertiesSet() throws Exception {
			throw new UnsupportedOperationException("TODO");
		}

		@Override
		public void destroy() throws Exception {
			throw new UnsupportedOperationException("TODO");
		}

		@Override
		public LocalMapStore get(String name) {
			throw new UnsupportedOperationException("TODO");
		}
		
	}
	/**
	 * Returns {@linkplain MapDBStore}. The store by default is a non-expiring map.
	 * @author Sutanu_Dalui
	 * @see MapDBStore
	 */
	public static class MapDBProvider implements MapStoreProvider{
		@Value("${txpipe.core.fileDbPath:file:./logs/db/file.db}")
		private String fileDbPath;
		
		private DB mapDB;
		@Override
		public void destroy() {
			mapDB.close();
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			mapDB = DBMaker.fileDB(ResourceUtils.getFile(fileDbPath))
			.transactionEnable()
			.fileMmapEnable()
			.fileMmapPreclearDisable()   // Make mmap file faster

	        // Unmap (release resources) file when its closed.
	        // That can cause JVM crash if file is accessed after it was unmapped
	        // (there is possible race condition).
			.cleanerHackEnable()
			.make();
		}

		@Override
		public LocalMapStore get(String name) {
			return new MapDBStore(mapDB, Long.MAX_VALUE, name);
		}
		
	}
}