package ru.itone.illya4gurenko.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import ru.itone.illya4gurenko.config.AppConfig;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * rest-клиент для взаимодействия с сервисом app-dispatcher
 */
@ApplicationScoped
public class DispatcherClient {

    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Inject
    public DispatcherClient(AppConfig appConfig, ObjectMapper objectMapper) {
        this(appConfig, objectMapper, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build());
    }

    /**
     * конструктор клиента диспетчера для внедрения зависимостей и тестов
     */
    public DispatcherClient(AppConfig appConfig, ObjectMapper objectMapper, HttpClient httpClient) {
        this.appConfig = appConfig;
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
    }

    /**
     * отправка post запроса к диспетчеру для получения источников данных
     */
    public DispatcherResponseBodyDto sendPost(Object requestBody) throws IOException, InterruptedException {
        String jsonPayload = objectMapper.writeValueAsString(requestBody);
        String authHeader = appConfig.getDispatcherBasicAuthHeader();

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(appConfig.getDispatcherUrl()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("Accept", "application/json");

        if (authHeader != null && !authHeader.isBlank()) {
            // передаем стандартный Authorization и кастомный Basic
            requestBuilder.header("Authorization", authHeader);
            requestBuilder.header("Basic", authHeader);
        }

        HttpRequest request = requestBuilder
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(
                request,
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
        );

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Dispatcher returned HTTP " + response.statusCode() + ": " + response.body());
        }

        return objectMapper.readValue(response.body(), DispatcherResponseBodyDto.class);
    }
}
