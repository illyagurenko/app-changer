package ru.itone.illya4gurenko.dto.dispatcher;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * блок с описанием ошибки от сервиса app-dispatcher
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherErrorDto(String code, String message) {}
