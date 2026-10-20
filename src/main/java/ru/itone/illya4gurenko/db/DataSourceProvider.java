package ru.itone.illya4gurenko.db;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import ru.itone.illya4gurenko.config.AppConfig;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceProvider {
    private static final HikariDataSource DATA_SOURCE;

    static {
        AppConfig config = AppConfig.getInstance();
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUsername());
        hikariConfig.setPassword(config.getDbPassword());
        hikariConfig.setMaximumPoolSize(config.getDbPoolSize());
        hikariConfig.setPoolName("AppChangerHikariPool");

        DATA_SOURCE = new HikariDataSource(hikariConfig);
    }

    public static DataSource getDataSource() {
        return DATA_SOURCE;
    }

    public static Connection getConnection() throws SQLException {
        return DATA_SOURCE.getConnection();
    }

    public static void close() {
        if (DATA_SOURCE != null && !DATA_SOURCE.isClosed()) {
            DATA_SOURCE.close();
        }
    }
}