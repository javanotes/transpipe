package org.reactiveminds.txpipe.core;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.reactiveminds.txpipe.core.api.LocalMapStore;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap.ExpiringKey;
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

	    public MapDBStore(InternalMapSupplier<ExpiringKey<String>, String> supp, long defaultMaxLifeTimeMillis) {
	        super(supp, defaultMaxLifeTimeMillis);
	        removeAllExpired();
	    }
	    
	}
	/**
	 * Serializer for {@linkplain ExpiringKey}
	 * @author Sutanu_Dalui
	 *
	 */
	private static class ExpiryKeySerializer implements Serializer<ExpiringKey<String>>{

		@Override
		public void serialize(DataOutput2 out, ExpiringKey<String> value) throws IOException {
			out.writeUTF(value.getKey());
			out.writeLong(value.getMaxLifeTimeMillis());
			out.writeLong(value.getStartTime());
		}

		@Override
		public ExpiringKey<String> deserialize(DataInput2 input, int available) throws IOException {
			ExpiringKey<String> key = new ExpiringKey<String>(input.readUTF(), input.readLong());
			key.setStartTime(input.readLong());
			return key;
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
		return new MapDBStore(() -> db.hashMap(mapName, new ExpiryKeySerializer(), Serializer.STRING_ASCII).createOrOpen(), Long.MAX_VALUE);
	}

	@Override
	public Class<?> getObjectType() {
		return LocalMapStore.class;
	}

}
