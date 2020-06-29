package com.yru.lib.util;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class LibUtils {

	private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

	public static <T> T convertMapToEntity(Map<String, Object> map, Class<T> clazz)
			throws JsonMappingException, JsonProcessingException {
		Gson gson = new Gson();
		T entity = (T) JACKSON_MAPPER.readValue(gson.toJson(map), clazz);
		return entity;
	}

	public static Map convertEntityToMap(Object entity) throws JsonMappingException, JsonProcessingException {
		Gson gson = new Gson();
		Map map = JACKSON_MAPPER.readValue(gson.toJson(entity), Map.class);
		return map;
	}

}
