package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dto.RequestEventDto;

import java.util.List;

/**
 * клиент для обращения в смежную систему app_controller
 */
@ApplicationScoped
public class AppControllerClient {
    private static final Logger log = LoggerFactory.getLogger(AppControllerClient.class);

    /**
     * эмулирует проверку и маршрутизацию счетов
     */
    public boolean checkAndRouteAccounts(List<RequestEventDto> events) {
        if (events == null || events.isEmpty()) {
            log.warn("app_controller: received empty events list");
            return false;
        }

        log.debug("app_controller: verifying {} accounts routing", events.size());
        for (RequestEventDto event : events) {
            if (event.systemAccount() == null || event.systemAccount().isBlank()) {
                log.warn("app_controller: invalid empty account found in event: {}", event.id());
                return false;
            }
        }
        return true;
    }
}