package ru.itone.illya4gurenko.controller;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dto.IoMsgDto;

import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class IoMsgResourceTest {

    private final IoMsgsDao ioMsgsDao = Mockito.mock(IoMsgsDao.class);
    private final IoMsgResource resource = createResource();

    private IoMsgResource createResource() {
        IoMsgResource r = new IoMsgResource();
        r.ioMsgsDao = ioMsgsDao;
        return r;
    }

    @Test
    @DisplayName("получение последней записи io_msgs при наличии данных")
    void testGetLatestSuccess() {
        IoMsgDto dto = new IoMsgDto(1L, 10L, "req-1", "KAFKA", "GRU", "OUT", "161", ZonedDateTime.now());
        when(ioMsgsDao.findLatest("OUT")).thenReturn(dto);

        Response response = resource.getLatest("OUT");

        assertAll(
                () -> assertEquals(200, response.getStatus()),
                () -> assertEquals(dto, response.getEntity())
        );
    }

    @Test
    @DisplayName("получение 404 при отсутствии записей io_msgs")
    void testGetLatestNotFound() {
        when(ioMsgsDao.findLatest("OUT")).thenReturn(null);

        Response response = resource.getLatest("OUT");

        assertEquals(404, response.getStatus());
    }

    @Test
    @DisplayName("получение записи io_msgs по requestId при наличии данных")
    void testGetByRequestIdSuccess() {
        IoMsgDto dto = new IoMsgDto(2L, 11L, "req-2", "KAFKA", "GRU", "IN", "162", ZonedDateTime.now());
        when(ioMsgsDao.findByRequestIdAndDir("req-2", "IN")).thenReturn(dto);

        Response response = resource.getByRequestId("req-2", "IN");

        assertAll(
                () -> assertEquals(200, response.getStatus()),
                () -> assertEquals(dto, response.getEntity())
        );
    }

    @Test
    @DisplayName("получение 400 при пустых параметрах поиска по requestId")
    void testGetByRequestIdBadRequest() {
        Response response = resource.getByRequestId("", "IN");

        assertEquals(400, response.getStatus());
    }

    @Test
    @DisplayName("получение 404 при отсутствии записи по requestId")
    void testGetByRequestIdNotFound() {
        when(ioMsgsDao.findByRequestIdAndDir("missing", "IN")).thenReturn(null);

        Response response = resource.getByRequestId("missing", "IN");

        assertEquals(404, response.getStatus());
    }
}
