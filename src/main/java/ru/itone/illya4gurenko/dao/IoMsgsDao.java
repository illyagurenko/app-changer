package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@ApplicationScoped
public class IoMsgsDao {

    public void insertIoMsg(Connection conn, long transId, String protocolType,
                            String msgType, String dir, String msg, String nodeId) throws SQLException {
        String sql = """
            INSERT INTO "ACC_APP_CHANGER"."APP_CHANGER_IO_MSGS" (
                "TRANS_ID", "PROTOCOL_TYPE", "MSG_TYPE", "DIR", "MSG", "INS_TS", "NODE_ID"
            ) VALUES (?, ?, ?, ?, ?, now(), ?)
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, transId);
            ps.setString(2, protocolType);
            ps.setString(3, msgType);
            ps.setString(4, dir);
            ps.setString(5, msg);
            ps.setString(6, nodeId);

            ps.executeUpdate();
        }
    }
}