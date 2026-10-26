package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
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

@ApplicationScoped
public class ProcedureDao {
    private static final Logger log = LoggerFactory.getLogger(ProcedureDao.class);

    private static final String CUR_IN_NAME = "cur_in";
    private static final String CUR_OUT_NAME = "cur_out";

    public ProcedureResult executeChangerDataAccount(Connection conn, List<RequestEventDto> events) throws SQLException {
        // наполняем временную таблицу входными данными
        setupTempInTable(conn, events);

        // открываем входной курсор по временной таблице
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DECLARE " + CUR_IN_NAME + " CURSOR FOR SELECT account, operation, amount, currency FROM temp_proc_in;");
        }

        // вызываем процедуру
        String callSql = "CALL \"ACC_APP_CHANGER\".changer_data_account(?, ?, ?, ?);";
        int outCode;
        String outMessage;

        try (CallableStatement cs = conn.prepareCall(callSql)) {
            cs.setString(1, CUR_IN_NAME);   // IN cur_in
            cs.setString(2, CUR_OUT_NAME);  // INOUT cur_out
            cs.registerOutParameter(2, Types.OTHER);
            cs.registerOutParameter(3, Types.INTEGER); // OUT code
            cs.registerOutParameter(4, Types.VARCHAR); // OUT message

            cs.execute();

            outCode = cs.getInt(3);
            outMessage = cs.getString(4);
        }

        // если процедура завершилась ошибкой на уровне СУБД
        if (outCode != 0) {
            log.error("Procedure changer_data_account returned error code: {}, message: {}", outCode, outMessage);
            return new ProcedureResult(outCode, outMessage, Map.of());
        }

        // вычитываем выходной курсор cur_out
        Map<String, ProcedureResult.CalculatedBalance> balances = new HashMap<>();
        try (Statement fetchStmt = conn.createStatement();
             ResultSet rs = fetchStmt.executeQuery("FETCH ALL FROM \"" + CUR_OUT_NAME + "\";")) {

            while (rs.next()) {
                String account = rs.getString("account");
                BigDecimal oldTbal = rs.getBigDecimal("oldTbal");
                BigDecimal newTbal = rs.getBigDecimal("newTbal");

                balances.put(account, new ProcedureResult.CalculatedBalance(oldTbal, newTbal));
            }
        }

        log.debug("Successfully fetched {} calculated balances from cursor", balances.size());
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