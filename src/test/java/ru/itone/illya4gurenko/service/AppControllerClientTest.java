package ru.itone.illya4gurenko.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dto.RequestEventDto;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppControllerClientTest {

    private final AppControllerClient client = new AppControllerClient();

    @Test
    @DisplayName("проверка маршрутизации при null списке событий")
    void testCheckAndRouteAccountsNull() {
        assertFalse(client.checkAndRouteAccounts(null));
    }

    @Test
    @DisplayName("проверка маршрутизации при пустом списке событий")
    void testCheckAndRouteAccountsEmpty() {
        assertFalse(client.checkAndRouteAccounts(Collections.emptyList()));
    }

    @Test
    @DisplayName("проверка маршрутизации при пустом счете аккаунта")
    void testCheckAndRouteAccountsBlankAccount() {
        RequestEventDto event = new RequestEventDto("1", " ", "RUB", BigDecimal.ONE, "PLUS");
        assertFalse(client.checkAndRouteAccounts(List.of(event)));
    }

    @Test
    @DisplayName("успешная проверка маршрутизации валидных счетов")
    void testCheckAndRouteAccountsSuccess() {
        RequestEventDto event = new RequestEventDto("1", "ACC_VALID", "RUB", BigDecimal.ONE, "PLUS");
        assertTrue(client.checkAndRouteAccounts(List.of(event)));
    }
}
