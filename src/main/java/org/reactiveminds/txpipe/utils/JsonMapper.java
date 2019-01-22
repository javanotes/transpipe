package org.reactiveminds.txpipe.utils;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.reactiveminds.txpipe.api.TransactionResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonMapper {

	private JsonMapper() {
	}
	private static class Wrapper{
		private static final ObjectMapper mapper = new ObjectMapper()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
				.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
				;
	}
	public static String makeResponse(TransactionResult result) {
		ObjectNode node = Wrapper.mapper.createObjectNode();
		node.put(result.getTxnId(), result.name());
		try {
			return Wrapper.mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}
	/**
	 * Serialize to a Json string.
	 * @param <T>
	 * @param object
	 * @return
	 */
	public static <T> String serialize(T object) {
		try {
			return Wrapper.mapper.writerFor(object.getClass()).writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}
	/**
	 * Marshall into a bean from a json string.
	 * @param json
	 * @param type
	 * @return
	 */
	public static <T> T deserialize(String json, Class<T> type) {
		try {
			return Wrapper.mapper.readerFor(type).readValue(json);
		} catch (Exception e) {
			throw new UncheckedIOException(new IOException(e));
		}
	}
	
	public static void main(String[] args) {
		TransactionResult r = TransactionResult.COMMIT;
		r.setTxnId("---");
		System.out.println(makeResponse(r));
	}
}
