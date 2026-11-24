package ru.itone.illya4gurenko.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование AESCryptoService")
class AESCryptoServiceTest {

    @Test
    @DisplayName("цикл шифрование -> расшифрование")
    void testEncryptDecryptCycle() {
        AESCryptoService cryptoService = AESCryptoService.getInstance();
        String originalPass = "SecretPassword123!";

        String encrypted = cryptoService.encrypt(originalPass);
        assertNotNull(encrypted);
        assertNotEquals(originalPass, encrypted);

        String decrypted = cryptoService.decrypt(encrypted);
        assertEquals(originalPass, decrypted);
    }

    @Test
    @DisplayName("формирование Basic Auth заголовка")
    void testBuildBasicAuthHeader() {
        AESCryptoService cryptoService = AESCryptoService.getInstance();
        String login = "APP_CHANGER";
        String password = "my_password";

        String encryptedPass = cryptoService.encrypt(password);
        String header = cryptoService.buildBasicAuthHeader(login, encryptedPass);

        assertNotNull(header);
        assertTrue(header.startsWith("Basic "));

        String base64Token = header.substring("Basic ".length());
        String decoded = new String(Base64.getDecoder().decode(base64Token));
        assertEquals("APP_CHANGER:my_password", decoded);
    }

    @Test
    @DisplayName("обработка некорректного Base64 при расшифровании")
    void testDecryptInvalidBase64() {
        AESCryptoService cryptoService = AESCryptoService.getInstance();
        assertThrows(RuntimeException.class, () -> cryptoService.decrypt("INVALID_NOT_BASE64!@#$"));
    }
}
