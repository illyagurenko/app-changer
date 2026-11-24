package ru.itone.illya4gurenko.dto.dispatcher.request;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import ru.itone.illya4gurenko.dto.dispatcher.DispatcherErrorDto;

/**
 * корневой json запроса к сервису app-dispatcher
 */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherRequestBodyDto(
        DispatcherRequestDataDto data,
        String status,
        DispatcherErrorDto error
) {}
