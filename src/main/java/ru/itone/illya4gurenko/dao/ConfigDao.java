package ru.itone.illya4gurenko.dao;

import ru.itone.illya4gurenko.db.DataSourceProvider;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConfigDao {

    public AppChangerConfigDto findBySystemId(String systemId) {
        String sql = """
            SELECT "ID", "SYSTEMID", "EVENT_TYPE", "ENTITY_TYPE",
                   "PROTOCOL_TYPE", "PROTOCOL_IN", "STATUS_IN",
                   "PROTOCOL_OUT", "STATUS_OUT"
            FROM "ACC_APP_CHANGER"."APP_CHANGER_CONFIG"
            WHERE "SYSTEMID" = ?
        """;

        try (Connection conn = DataSourceProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, systemId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new AppChangerConfigDto(
                            rs.getLong("ID"),
                            rs.getString("SYSTEMID"),
                            rs.getString("EVENT_TYPE"),
                            rs.getString("ENTITY_TYPE"),
                            rs.getString("PROTOCOL_TYPE"),
                            rs.getString("PROTOCOL_IN"),
                            rs.getInt("STATUS_IN"),
                            rs.getString("PROTOCOL_OUT"),
                            rs.getInt("STATUS_OUT")
                    );
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read APP_CHANGER_CONFIG for systemId=" + systemId, e);
        }
        return null;
    }
}
