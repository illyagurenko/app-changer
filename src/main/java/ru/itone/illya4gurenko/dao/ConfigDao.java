package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import ru.itone.illya4gurenko.entity.AppChangerConfig;

/**
 * работа с таблицей конфигурации
 */
@ApplicationScoped
public class ConfigDao {

    public AppChangerConfig findBySystemId(String systemId) {
        return AppChangerConfig.find("systemId", systemId).firstResult();
    }
}