package ru.itone.illya4gurenko.dao;

import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.hibernate.jdbc.ReturningWork;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dto.ProcedureResult;
import ru.itone.illya4gurenko.dto.RequestEventDto;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class ProcedureDaoTest {

    @Test
    @DisplayName("успешное выполнение хранимой процедуры пересчета балансов")
    void testExecuteChangerDataAccountSuccess() throws Exception {
        EntityManager em = Mockito.mock(EntityManager.class);
        Session session = Mockito.mock(Session.class);
        Connection conn = Mockito.mock(Connection.class);
        Statement stmt = Mockito.mock(Statement.class);
        CallableStatement cs = Mockito.mock(CallableStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);

        when(em.unwrap(Session.class)).thenReturn(session);
        when(session.doReturningWork(any())).thenAnswer(invocation -> {
            ReturningWork<ProcedureResult> work = invocation.getArgument(0);
            return work.execute(conn);
        });

        when(conn.createStatement()).thenReturn(stmt);
        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(cs.getInt(3)).thenReturn(0);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("account")).thenReturn("ACC_1");
        when(rs.getBigDecimal("oldTbal")).thenReturn(BigDecimal.ONE);
        when(rs.getBigDecimal("newTbal")).thenReturn(BigDecimal.TEN);

        ProcedureDao dao = new ProcedureDao();
        dao.em = em;

        RequestEventDto event = new RequestEventDto("1", "ACC_1", "RUB", BigDecimal.ONE, "PLUS");
        ProcedureResult result = dao.executeChangerDataAccount(List.of(event));

        assertEquals(0, result.code());
    }

    @Test
    @DisplayName("обработка возврата кода ошибки из хранимой процедуры")
    void testExecuteChangerDataAccountProcedureErrorCode() throws Exception {
        EntityManager em = Mockito.mock(EntityManager.class);
        Session session = Mockito.mock(Session.class);
        Connection conn = Mockito.mock(Connection.class);
        Statement stmt = Mockito.mock(Statement.class);
        CallableStatement cs = Mockito.mock(CallableStatement.class);

        when(em.unwrap(Session.class)).thenReturn(session);
        when(session.doReturningWork(any())).thenAnswer(invocation -> {
            ReturningWork<ProcedureResult> work = invocation.getArgument(0);
            return work.execute(conn);
        });

        when(conn.createStatement()).thenReturn(stmt);
        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(cs.getInt(3)).thenReturn(500);
        when(cs.getString(4)).thenReturn("Internal DB Error");

        ProcedureDao dao = new ProcedureDao();
        dao.em = em;

        RequestEventDto event = new RequestEventDto("1", "ACC_1", "RUB", BigDecimal.ONE, "PLUS");
        ProcedureResult result = dao.executeChangerDataAccount(List.of(event));

        assertEquals(500, result.code());
    }
}

