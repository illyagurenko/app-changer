package ru.itone.illya4gurenko.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * утилиты для сериализации и десериализации json
 */
public final class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonUtils() {}

    /**
     * получение настроенного mapper
     */
    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    /**
     * чтение json строки в объект
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("failed to deserialize json: " + json, e);
        }
    }

    /**
     * запись объекта в json строку
     */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize object to json", e);
        }
    }
}