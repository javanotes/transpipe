package org.reactiveminds.txpipe.utils;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper {

	private static class Wrapper{
		public static final ObjectMapper mapper = new ObjectMapper()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				;
	}
	/**
	 * 	
	 * @param object
	 * @return
	 */
	public <T> String toJson(T object) {
		try {
			return Wrapper.mapper.writerFor(object.getClass()).writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}
	/**
	 * 
	 * @param json
	 * @param type
	 * @return
	 */
	public <T> T toObject(String json, Class<T> type) {
		try {
			return Wrapper.mapper.readerFor(type).readValue(json);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
