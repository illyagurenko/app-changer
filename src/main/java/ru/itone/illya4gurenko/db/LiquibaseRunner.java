package ru.itone.illya4gurenko.db;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class LiquibaseRunner {
    private static final Logger log = LoggerFactory.getLogger(LiquibaseRunner.class);

    public static void runMigrations() {
        log.info("starting liquibase migrations on postgreSQL");
        try (Connection conn = DataSourceProvider.getConnection()) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));

            Liquibase liquibase = new Liquibase(
                    "db/changelog/db.changelog-master.yaml",
                    new ClassLoaderResourceAccessor(),
                    database
            );

            liquibase.update("");
            log.info("liquibase migrations applied success");
        } catch (Exception e) {
            log.error("liquibase migration failed", e);
            throw new RuntimeException("database migration failed", e);
        }
    }
}
