package org.reactiveminds.txpipe.utils;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper {

	private JsonMapper() {
	}
	private static class Wrapper{
		private static final ObjectMapper mapper = new ObjectMapper()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				;
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
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
