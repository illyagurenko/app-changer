package ru.itone.illya4gurenko.dto.dispatcher.request;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/**
 * тело данных со списком счетов для запроса к app-dispatcher
 */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherRequestDataDto(
        String systemId, // "APP_CHANGER"
        String requestId,
        List<String> account
) {}
