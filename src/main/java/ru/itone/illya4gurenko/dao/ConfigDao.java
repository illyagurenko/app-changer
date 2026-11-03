package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import ru.itone.illya4gurenko.entity.AppChangerConfig;

/**
 * работа с конфигурацией систем в базе данных
 */
@ApplicationScoped
public class ConfigDao {

    /**
     * поиск конфигурации по коду системы
     */
    @Transactional
    public AppChangerConfig findBySystemId(String systemId) {
        return AppChangerConfig.find("systemId", systemId).firstResult();
    }
}