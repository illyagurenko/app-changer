package ru.itone.illya4gurenko.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TransDao {

    /**
     * Создает новую запись транзакции в статусе RECEIVED.
     * Возвращает сгенерированный ID.
     */
    public long insertReceived(Connection conn, String protocolType, String systemId,
                               String requestId, String eventType, String reqData) throws SQLException {
        String sql = """
            INSERT INTO "ACC_APP_CHANGER"."APP_CHANGER_TRANS" (
                "PROTOCOL_TYPE", "SYSTEM_ID", "REQUEST_ID", "EVENT_TYPE",
                "req_DATA", "STATUS", "INS_TS", "UPD_TS"
            ) VALUES (?, ?, ?, ?, ?, 'RECEIVED', now(), now())
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, protocolType);
            ps.setString(2, systemId);
            ps.setString(3, requestId);
            ps.setString(4, eventType);
            ps.setString(5, reqData);

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to retrieve generated ID for APP_CHANGER_TRANS");
    }
}