package ru.itone.illya4gurenko.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.config.AppConfig;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherDatasourceDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseDataDto;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@DisplayName("Тестирование DispatcherClient")
class DispatcherClientTest {

    @Test
    @DisplayName("успешная отправка запроса и десериализация ответа")
    @SuppressWarnings("unchecked")
    void testSendPostSuccess() throws Exception {
        AppConfig appConfig = Mockito.mock(AppConfig.class);
        when(appConfig.getDispatcherUrl()).thenReturn("http://localhost:8085/get-datasoure-by-account");
        when(appConfig.getDispatcherBasicAuthHeader()).thenReturn("Basic dXNlcjpwYXNz");

        ObjectMapper mapper = new ObjectMapper();
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        HttpResponse<String> httpResponse = Mockito.mock(HttpResponse.class);

        DispatcherResponseBodyDto mockRespDto = new DispatcherResponseBodyDto(
                "req-uuid", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:postgresql://db:5432/test", List.of("ACC1"))))
        );
        String jsonResponse = mapper.writeValueAsString(mockRespDto);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(jsonResponse);
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        DispatcherClient client = new DispatcherClient(appConfig, mapper, httpClient);
        DispatcherResponseBodyDto result = client.sendPost(Map.of("key", "val"));

        assertNotNull(result);
        assertEquals("req-uuid", result.requestId());
        assertEquals("Success", result.status());
        assertEquals(1, result.data().datasource().size());
        assertEquals("jdbc:postgresql://db:5432/test", result.data().datasource().get(0).jdbcUrl());
    }

    @Test
    @DisplayName("обработка ошибки HTTP статуса от диспетчера")
    @SuppressWarnings("unchecked")
    void testSendPostHttpError() throws Exception {
        AppConfig appConfig = Mockito.mock(AppConfig.class);
        when(appConfig.getDispatcherUrl()).thenReturn("http://localhost:8085/get-datasoure-by-account");

        ObjectMapper mapper = new ObjectMapper();
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        HttpResponse<String> httpResponse = Mockito.mock(HttpResponse.class);

        when(httpResponse.statusCode()).thenReturn(500);
        when(httpResponse.body()).thenReturn("Internal Server Error");
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        DispatcherClient client = new DispatcherClient(appConfig, mapper, httpClient);
        assertThrows(IOException.class, () -> client.sendPost(Map.of()));
    }
}
