package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dto.ProcedureResult;
import ru.itone.illya4gurenko.dto.RequestEventDto;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * вызов хранимой процедуры в бд
 */
@ApplicationScoped
public class ProcedureDao {
    private static final Logger log = LoggerFactory.getLogger(ProcedureDao.class);

    private static final String CUR_IN_NAME = "cur_in";
    private static final String CUR_OUT_NAME = "cur_out";

    @Inject
    EntityManager em;

    /**
     * вызывает процедуру changer_data_account в рамках текущей сессии hibernate
     */
    public ProcedureResult executeChangerDataAccount(List<RequestEventDto> events) {
        Session session = em.unwrap(Session.class);
        return session.doReturningWork(conn -> {
            try {
                return executeInternal(conn, events);
            } catch (SQLException e) {
                log.error("sql error during procedure call", e);
                return new ProcedureResult(1, e.getMessage(), Map.of());
            }
        });
    }

    private ProcedureResult executeInternal(Connection conn, List<RequestEventDto> events) throws SQLException {
        setupTempInTable(conn, events);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DECLARE " + CUR_IN_NAME + " CURSOR FOR SELECT account, operation, amount, currency FROM temp_proc_in;");
        }

        String callSql = "CALL \"ACC_APP_CHANGER\".changer_data_account(?, ?, ?, ?);";
        int outCode;
        String outMessage;

        try (CallableStatement cs = conn.prepareCall(callSql)) {
            cs.setString(1, CUR_IN_NAME);
            cs.setString(2, CUR_OUT_NAME);
            cs.setInt(3, 0);
            cs.setNull(4, Types.VARCHAR);

            cs.registerOutParameter(2, Types.VARCHAR);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.registerOutParameter(4, Types.VARCHAR);

            cs.execute();

            outCode = cs.getInt(3);
            outMessage = cs.getString(4);
        }

        if (outCode != 0) {
            log.error("procedure returned error code: {}, message: {}", outCode, outMessage);
            return new ProcedureResult(outCode, outMessage, Map.of());
        }

        Map<String, ProcedureResult.CalculatedBalance> balances = new HashMap<>();
        try (Statement fetchStmt = conn.createStatement();
             ResultSet rs = fetchStmt.executeQuery("FETCH ALL FROM \"" + CUR_OUT_NAME + "\";")) {

            while (rs.next()) {
                balances.put(
                        rs.getString("account"),
                        new ProcedureResult.CalculatedBalance(rs.getBigDecimal("oldTbal"), rs.getBigDecimal("newTbal"))
                );
            }
        }

        return new ProcedureResult(0, null, balances);
    }

    private void setupTempInTable(Connection conn, List<RequestEventDto> events) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_proc_in (
                    account VARCHAR,
                    operation VARCHAR,
                    amount NUMERIC(23, 3),
                    currency VARCHAR
                ) ON COMMIT DROP;
                DELETE FROM temp_proc_in;
            """);
        }

        String insertSql = "INSERT INTO temp_proc_in (account, operation, amount, currency) VALUES (?, ?, ?, ?);";
        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (RequestEventDto event : events) {
                ps.setString(1, event.systemAccount());
                ps.setString(2, event.operation());
                ps.setBigDecimal(3, event.xalfa() != null ? event.xalfa() : BigDecimal.ZERO);
                ps.setString(4, event.currency());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}