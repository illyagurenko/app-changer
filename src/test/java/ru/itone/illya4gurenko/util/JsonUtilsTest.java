package ru.itone.illya4gurenko.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dto.ErrorDto;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    @DisplayName("получение сконфигурированного ObjectMapper")
    void testGetMapper() {
        assertNotNull(JsonUtils.getMapper());
    }

    @Test
    @DisplayName("сериализация объекта в json")
    void testToJson() {
        ErrorDto dto = new ErrorDto("400", "Bad request");
        String json = JsonUtils.toJson(dto);
        assertTrue(json.contains("400"));
    }

    @Test
    @DisplayName("успешная десериализация из json в объект")
    void testFromJson() {
        String json = "{\"code\":\"200\",\"message\":\"OK\"}";
        ErrorDto dto = JsonUtils.fromJson(json, ErrorDto.class);
        assertEquals("200", dto.code());
    }

    @Test
    @DisplayName("выброс исключения при десериализации некорректного json")
    void testFromJsonException() {
        assertThrows(RuntimeException.class, () -> JsonUtils.fromJson("invalid-json", ErrorDto.class));
    }
}

