package ru.itone.illya4gurenko.dao;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerIoMsg;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusTest
class PanacheDaoTest {

    @Inject
    TransDao transDao;

    @Inject
    IoMsgsDao ioMsgsDao;

    @Inject
    ConfigDao configDao;

    @BeforeEach
    @Transactional
    void cleanUp() {
        AppChangerTrans.deleteAll();
        AppChangerIoMsg.deleteAll();
    }

    @Test
    @Transactional
    @DisplayName("сохранение входящей транзакции")
    void testInsertReceivedAndFind() {
        AppChangerTrans trans = transDao.insertReceived("JSON", "GRU", "req-test-1", "BALANCE", "{}");
        assertNotNull(trans.id);
    }

    @Test
    @Transactional
    @DisplayName("поиск транзакции по id")
    void testFindById() {
        AppChangerTrans trans = transDao.insertReceived("JSON", "GRU", "req-test-2", "BALANCE", "{}");
        AppChangerTrans found = transDao.findById(trans.id);
        assertNotNull(found);
    }

    @Test
    @Transactional
    @DisplayName("сохранение записи io_msgs")
    void testInsertIoMsg() {
        ioMsgsDao.insertIoMsg(1L, "JSON", "GRU", "IN", "{}", "162");
        assertNotNull(ioMsgsDao);
    }

    @Test
    @Transactional
    @DisplayName("поиск конфигурации по несуществующему systemId")
    void testConfigDao() {
        AppChangerConfig config = configDao.findBySystemId("NON_EXISTING");
        assertNull(config);
    }

    @Test
    @Transactional
    @DisplayName("захват транзакции на обработку при пустой базе")
    void testAcquireNextForProcessingWhenEmpty() {
        AppChangerTrans trans = transDao.acquireNextForProcessing();
        assertNull(trans);
    }

    @Test
    @Transactional
    @DisplayName("захват транзакции на обработку при наличии записей")
    void testAcquireNextForProcessingWhenPresent() {
        transDao.insertReceived("JSON", "GRU", "req-test-proc", "BALANCE", "{}");
        AppChangerTrans acquired = transDao.acquireNextForProcessing();
        assertNotNull(acquired);
    }

    @Test
    @Transactional
    @DisplayName("захват транзакции на отправку при пустой базе")
    void testAcquireNextForSendingWhenEmpty() {
        AppChangerTrans trans = transDao.acquireNextForSending();
        assertNull(trans);
    }

    @Test
    @Transactional
    @DisplayName("захват транзакции на отправку при наличии записей")
    void testAcquireNextForSendingWhenPresent() {
        AppChangerTrans trans = transDao.insertReceived("JSON", "GRU", "req-test-send", "BALANCE", "{}");
        trans.status = "READY_TO_SEND";
        AppChangerTrans acquired = transDao.acquireNextForSending();
        assertNotNull(acquired);
    }

    @Test
    @Transactional
    @DisplayName("поиск последней записи io_msgs по направлению и requestId")
    void testFindLatestIoMsgByDir() {
        AppChangerTrans trans = transDao.insertReceived("KAFKA", "GRU", "req-io-latest", "BALANCE", "{}");
        ioMsgsDao.insertIoMsg(trans.id, "KAFKA", "GRU", "IN", "{}", "161");
        ioMsgsDao.insertIoMsg(trans.id, "KAFKA", "GRU", "OUT", "{}", "161");

        var latestOut = ioMsgsDao.findLatest("OUT");
        var latestAny = ioMsgsDao.findLatest(null);
        var byReq = ioMsgsDao.findByRequestIdAndDir("req-io-latest", "IN");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertNotNull(latestOut),
                () -> org.junit.jupiter.api.Assertions.assertEquals("req-io-latest", latestOut.requestId()),
                () -> org.junit.jupiter.api.Assertions.assertEquals("OUT", latestOut.dir()),
                () -> assertNotNull(latestAny),
                () -> assertNotNull(byReq),
                () -> org.junit.jupiter.api.Assertions.assertEquals("IN", byReq.dir())
        );
    }
}
