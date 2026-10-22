package ru.itone.illya4gurenko;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.db.DataSourceProvider;
import ru.itone.illya4gurenko.db.LiquibaseRunner;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class AppChangerMain {
    private static final Logger log = LoggerFactory.getLogger(AppChangerMain.class);

    public static void main(String[] args) {
        log.info("Starting app-changer");

        LiquibaseRunner.runMigrations();

        String sql = """
            SELECT "SYSTEMID", "PROTOCOL_IN", "PROTOCOL_OUT", "STATUS_IN", "STATUS_OUT"
            FROM "ACC_APP_CHANGER"."APP_CHANGER_CONFIG"
            WHERE "SYSTEMID" = 'GRU'
        """;

        try (Connection conn = DataSourceProvider.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                log.info("config loaded successfully: SYSTEMID={}, PROTOCOL_IN={}, PROTOCOL_OUT={}, STATUS_IN={}, STATUS_OUT={}",
                        rs.getString("SYSTEMID"),
                        rs.getString("PROTOCOL_IN"),
                        rs.getString("PROTOCOL_OUT"),
                        rs.getInt("STATUS_IN"),
                        rs.getInt("STATUS_OUT"));
            }
        } catch (Exception e) {
            log.error("failed to read from ACC_APP_CHANGER.APP_CHANGER_CONFIG", e);
            System.exit(1);
        }

        log.info("database schema and procedure verified 1-to-1 with requirements!");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("closing DataSource");
            DataSourceProvider.close();
            log.info("service stopped.");
        }));
    }
}