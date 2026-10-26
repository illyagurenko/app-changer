package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import ru.itone.illya4gurenko.dto.TransRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
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
                "REQ_DATA", "STATUS", "INS_TS", "UPD_TS"
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

    /**
     * выбирает пачку транзакций в статусе RECEIVED с блокировкой от других воркеров
     */
    public List<TransRecord> fetchBatchForProcessing(Connection conn, int limit) throws SQLException {
        String sql = """
        SELECT "ID", "REQUEST_ID", "SYSTEM_ID", "EVENT_TYPE", "PROTOCOL_TYPE", "REQ_DATA"
        FROM "ACC_APP_CHANGER"."APP_CHANGER_TRANS"
        WHERE "STATUS" = 'RECEIVED'
        ORDER BY "ID" ASC
        LIMIT ?
        FOR UPDATE SKIP LOCKED
    """;

        List<TransRecord> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new TransRecord(
                            rs.getLong("ID"),
                            rs.getString("REQUEST_ID"),
                            rs.getString("SYSTEM_ID"),
                            rs.getString("EVENT_TYPE"),
                            rs.getString("PROTOCOL_TYPE"),
                            rs.getString("REQ_DATA")
                    ));
                }
            }
        }
        return list;
    }

    /**
     * переводит транзакцию в статус IN_PROCESS
     */
    public void updateStatus(Connection conn, long transId, String status) throws SQLException {
        String sql = """
        UPDATE "ACC_APP_CHANGER"."APP_CHANGER_TRANS"
        SET "STATUS" = ?, "UPD_TS" = now()
        WHERE "ID" = ?
    """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, transId);
            ps.executeUpdate();
        }
    }

    /**
     * сохраняет сформированный RESP_DATA и выставляет финальный статус обработки
     */
    public void updateResponse(Connection conn, long transId, String respData, String status, String respCode, String respDesc) throws SQLException {
        String sql = """
        UPDATE "ACC_APP_CHANGER"."APP_CHANGER_TRANS"
        SET "RESP_DATA" = ?, "STATUS" = ?, "RESP_CODE" = ?, "RESP_DESC" = ?, "UPD_TS" = now()
        WHERE "ID" = ?
    """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, respData);
            ps.setString(2, status);
            ps.setString(3, respCode);
            ps.setString(4, respDesc);
            ps.setLong(5, transId);
            ps.executeUpdate();
        }
    }
}