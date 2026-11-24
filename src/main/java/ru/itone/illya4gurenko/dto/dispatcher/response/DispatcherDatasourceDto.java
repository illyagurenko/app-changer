package ru.itone.illya4gurenko.dto.dispatcher.response;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/**
 * элемент списка источников данных с привязанными счетами
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DispatcherDatasourceDto(
        @JsonProperty("jdbc_url") @JsonAlias({"jdbc_url", "jdbcUrl"})
        String jdbcUrl,
        @JsonProperty("account") @JsonAlias({"account", "accounts"})
        List<String> account
) {}
