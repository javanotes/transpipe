package org.reactiveminds.txpipe.store;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.reactiveminds.txpipe.utils.SelfExpiringHashMap.ExpiringKey;

/**
 * Serializer for {@linkplain ExpiringKey}
 * @author Sutanu_Dalui
 *
 */
class ExpiryKeySerializer implements Serializer<ExpiringKey<String>>{

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