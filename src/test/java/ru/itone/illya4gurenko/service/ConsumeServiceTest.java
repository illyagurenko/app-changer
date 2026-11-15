package ru.itone.illya4gurenko.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumeServiceTest {

    private final TransDao transDao = Mockito.mock(TransDao.class);
    private final IoMsgsDao ioMsgsDao = Mockito.mock(IoMsgsDao.class);

    private ConsumeService service;

    @BeforeEach
    void setUp() {
        service = new ConsumeService();
        service.transDao = transDao;
        service.ioMsgsDao = ioMsgsDao;
        service.nodeId = "162";
    }

    @Test
    @DisplayName("пропуск сохранения при пустом входящем json")
    void testReadAndSaveDbBlankJson() {
        AppChangerConfig config = new AppChangerConfig();
        service.readAndSaveDb(config, "", "key-1");

        verify(transDao, never()).insertReceived(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("успешное чтение и сохранение валидного json в базу")
    void testReadAndSaveDbValidJson() {
        AppChangerConfig config = new AppChangerConfig();
        config.protocolType = "JSON";
        config.systemId = "GRU";
        config.eventType = "BALANCE";

        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 100L;
        when(transDao.insertReceived(any(), any(), any(), any(), any())).thenReturn(trans);

        String json = "{\"requestId\":\"req-val\",\"systemId\":\"SMART\",\"eventType\":\"EVENT\"}";
        service.readAndSaveDb(config, json, "key-val");

        verify(ioMsgsDao).insertIoMsg(eq(100L), any(), any(), eq("IN"), any(), eq("162"));
    }

    @Test
    @DisplayName("сохранение невалидного json с фоллбэком на kafka ключ")
    void testReadAndSaveDbInvalidJsonFallback() {
        AppChangerConfig config = new AppChangerConfig();
        config.protocolType = "JSON";
        config.systemId = "GRU";

        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 200L;
        when(transDao.insertReceived(any(), any(), any(), any(), any())).thenReturn(trans);

        service.readAndSaveDb(config, "non-json-content", "key-fallback");

        verify(transDao).insertReceived(eq("JSON"), eq("GRU"), eq("key-fallback"), any(), eq("non-json-content"));
    }
}

