package ru.itone.illya4gurenko.dto.dispatcher.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/**
 * тело данных ответа от app-dispatcher со списком источников
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherResponseDataDto(
        List<DispatcherDatasourceDto> datasource
) {}
