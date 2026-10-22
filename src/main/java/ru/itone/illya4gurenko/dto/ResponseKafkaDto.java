package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

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