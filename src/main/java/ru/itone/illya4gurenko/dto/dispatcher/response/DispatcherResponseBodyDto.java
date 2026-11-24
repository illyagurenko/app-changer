package ru.itone.illya4gurenko.dto.dispatcher.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import ru.itone.illya4gurenko.dto.dispatcher.DispatcherErrorDto;

/**
 * корневой json ответа от сервиса app-dispatcher
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherResponseBodyDto(
        String requestId,
        String systemId,
        String status,
        DispatcherErrorDto error,
        DispatcherResponseDataDto data
) {}
