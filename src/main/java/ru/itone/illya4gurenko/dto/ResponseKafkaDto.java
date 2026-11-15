package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;

/**
 * корневой json исходящего ответа для отправки в кафку
 */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResponseKafkaDto(
        Long actualTimestamp,
        String systemId,
        String requestId,
        String eventType,
        String entityType,
        String status,
        ErrorDto error,
        List<ResponseEventDto> events
) {}
