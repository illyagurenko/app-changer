package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * корневой json входящего сообщения из кафки
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestKafkaDto(
        Long actualTimestamp,
        String systemId,
        String requestId,
        String eventType,
        String entityType,
        List<RequestEventDto> events
) {}