package org.reactiveminds.txpipe.utils;

import java.io.IOException;

import org.reactiveminds.txpipe.core.dto.TransactionResult;
import org.reactiveminds.txpipe.err.DataSerializationException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonMapper {

	private JsonMapper() {
	}
	private static class Wrapper{
		private static final ObjectMapper mapper = new ObjectMapper()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				//.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
				.enable(SerializationFeature.INDENT_OUTPUT)
				//.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
				;
	}
	/**
	 * 
	 * @param result
	 * @return
	 */
	public static String makeResponse(TransactionResult result) {
		return toString(result);
	}
	public static <T> String toString(T object) {
		try {
			return Wrapper.mapper.writerFor(object.getClass()).withDefaultPrettyPrinter().writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new DataSerializationException(e);
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
			throw new DataSerializationException(e);
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
			throw new DataSerializationException(e instanceof IOException ? (IOException)e : new IOException(e));
		}
	}
	
}
