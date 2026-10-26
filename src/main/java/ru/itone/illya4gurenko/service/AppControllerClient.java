package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dto.RequestEventDto;

import java.util.List;

@ApplicationScoped
public class AppControllerClient {
    private static final Logger log = LoggerFactory.getLogger(AppControllerClient.class);

    /**
     * эмулирует обращение в APP_CONTROLLER для валидации и маршрутизации счетов
     */
    public boolean checkAndRouteAccounts(List<RequestEventDto> events) {
        if (events == null || events.isEmpty()) {
            log.warn("APP_CONTROLLER: received empty events list");
            return false;
        }

        log.debug("APP_CONTROLLER: verifying {} accounts routing...", events.size());
        for (RequestEventDto event : events) {
            if (event.systemAccount() == null || event.systemAccount().isBlank()) {
                log.warn("APP_CONTROLLER: invalid empty account found in event: {}", event.id());
                return false;
            }
        }
        // все счета успешно маршрутизируются в схему ACC_APP_CHANGER
        return true;
    }
}