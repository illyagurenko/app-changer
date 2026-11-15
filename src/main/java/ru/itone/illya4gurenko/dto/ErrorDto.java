package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * блок с описанием ошибки
 */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorDto(
        String code,
        String message
) {}
