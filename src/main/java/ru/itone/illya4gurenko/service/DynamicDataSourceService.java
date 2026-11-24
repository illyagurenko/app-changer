package ru.itone.illya4gurenko.service;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

/**
 * сервис управления динамическими подключениями к базам данных по jdbc_url
 */
@ApplicationScoped
public class DynamicDataSourceService {
    private static final Logger log = LoggerFactory.getLogger(DynamicDataSourceService.class);

    @Inject
    AgroalDataSource defaultDataSource;

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    Optional<String> defaultJdbcUrl;

    @ConfigProperty(name = "quarkus.datasource.username")
    Optional<String> defaultUsername;

    @ConfigProperty(name = "quarkus.datasource.password")
    Optional<String> defaultPassword;

    /**
     * получение соединения к базе данных по jdbcUrl с быстрым таймаутом
     */
    public Connection getConnection(String jdbcUrl) throws SQLException {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("JDBC URL is null or empty");
        }

        if (isDefaultDatasource(jdbcUrl)) {
            return defaultDataSource.getConnection();
        }

        Properties props = new Properties();
        defaultUsername.ifPresent(u -> {
            if (!u.isBlank()) props.setProperty("user", u);
        });
        defaultPassword.ifPresent(p -> {
            if (!p.isBlank()) props.setProperty("password", p);
        });
        // Быстрые таймауты (в секундах) для быстрого отбоя недоступных баз
        props.setProperty("connectTimeout", "3");
        props.setProperty("socketTimeout", "5");
        props.setProperty("loginTimeout", "3");

        log.debug("establishing connection to dynamic db: {}", jdbcUrl);
        return DriverManager.getConnection(jdbcUrl.trim(), props);
    }

    /**
     * проверка совпадения адреса с дефолтным источником данных приложения
     */
    private boolean isDefaultDatasource(String jdbcUrl) {
        if (defaultJdbcUrl.isEmpty() || defaultDataSource == null) {
            return false;
        }
        String configured = defaultJdbcUrl.get().trim();
        String target = jdbcUrl.trim();
        if (configured.equalsIgnoreCase(target)) {
            return true;
        }
        if (target.contains("/changer_db") && configured.contains("/changer_db")) {
            return true;
        }
        if (target.contains("h2:mem:changer_test") && configured.contains("h2:mem:changer_test")) {
            return true;
        }
        return false;
    }
}
