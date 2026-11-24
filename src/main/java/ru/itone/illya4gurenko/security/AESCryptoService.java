package ru.itone.illya4gurenko.security;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;

/**
 * сервис симметричного шифрования и дешифрования данных по алгоритму aes
 */
@ApplicationScoped
public class AESCryptoService {

    private static final Logger log = LoggerFactory.getLogger(AESCryptoService.class);
    private static final String ALGORITHM = "AES";
    private static final String DEFAULT_BASE64_KEY = "vtkFKhCzuberXpROnkV/2Q==";

    private static final AESCryptoService INSTANCE = new AESCryptoService();

    private final SecretKey secretKey;
    private boolean isDisableCrypto = false;

    /**
     * получение синглтон-экземпляра сервиса
     */
    public static AESCryptoService getInstance() {
        return INSTANCE;
    }

    public AESCryptoService() {
        this.secretKey = initSecretKey();
    }

    /**
     * инициализация секретного ключа aes из файла, свойства или дефолтного значения
     */
    private SecretKey initSecretKey() {
        try {
            Config config = ConfigProvider.getConfig();

            Optional<String> keyPathOpt = config.getOptionalValue("crypto.key.path", String.class);
            if (keyPathOpt.isPresent() && !keyPathOpt.get().isBlank()) {
                Path keyFilePath = Paths.get(keyPathOpt.get().trim());
                if (Files.exists(keyFilePath)) {
                    String base64Key = Files.readString(keyFilePath, StandardCharsets.UTF_8).trim();
                    byte[] decodedKey = Base64.getDecoder().decode(base64Key);
                    log.info("AES Crypto Service initialized from file: {}", keyFilePath);
                    return new SecretKeySpec(decodedKey, ALGORITHM);
                } else {
                    log.warn("Crypto key file not found at: {}. Trying fallback options.", keyPathOpt.get());
                }
            }

            Optional<String> directKeyOpt = config.getOptionalValue("crypto.key", String.class);
            if (directKeyOpt.isPresent() && !directKeyOpt.get().isBlank()) {
                byte[] keyBytes = decodeOrDeriveKey(directKeyOpt.get().trim());
                log.info("AES Crypto Service initialized from 'crypto.key' property.");
                return new SecretKeySpec(keyBytes, ALGORITHM);
            }

            log.info("AES Crypto Service initialized with default key.");
            byte[] defaultKeyBytes = Base64.getDecoder().decode(DEFAULT_BASE64_KEY);
            return new SecretKeySpec(defaultKeyBytes, ALGORITHM);

        } catch (Exception e) {
            log.error("Failed to initialize AES encryption key. Encryption disabled.", e);
            this.isDisableCrypto = true;
            return null;
        }
    }

    /**
     * деривация ключа из строки или декодирование base64
     */
    private byte[] decodeOrDeriveKey(String rawKey) {
        try {
            byte[] decoded = Base64.getDecoder().decode(rawKey);
            if (decoded.length == 16 || decoded.length == 24 || decoded.length == 32) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // не Base64
        }
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha.digest(rawKey.getBytes(StandardCharsets.UTF_8));
            byte[] key16 = new byte[16];
            System.arraycopy(digest, 0, key16, 0, 16);
            return key16;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /**
     * расшифрование base64 строки алгоритмом aes
     */
    public String decrypt(String encryptedText) {
        if (isDisableCrypto || secretKey == null || encryptedText == null || encryptedText.isBlank()) {
            return encryptedText;
        }

        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedText.trim());
            byte[] decryptedBytes = decryptBytes(decoded, secretKey);
            return new String(decryptedBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Error during AES decryption: {}", e.getMessage());
            throw new RuntimeException("Error during AES decryption", e);
        }
    }

    /**
     * зашифрование строки алгоритмом aes в base64
     */
    public String encrypt(String decryptedText) {
        if (isDisableCrypto || secretKey == null || decryptedText == null) {
            return decryptedText;
        }

        try {
            byte[] encryptedBytes = encryptBytes(decryptedText.getBytes(StandardCharsets.UTF_8), secretKey);
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            log.error("Error during AES encryption: {}", e.getMessage());
            throw new RuntimeException("Error during AES encryption", e);
        }
    }

    /**
     * формирование заголовка basic auth: Authorization: Basic {base64(login:password)}
     */
    public String buildBasicAuthHeader(String username, String encryptedPassword) {
        String plainPassword = decrypt(encryptedPassword);
        String token = (username != null ? username : "") + ":" + (plainPassword != null ? plainPassword : "");
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] decryptBytes(byte[] encryptedBytes, SecretKey key)
            throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(encryptedBytes);
    }

    private byte[] encryptBytes(byte[] decryptedBytes, SecretKey key)
            throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(decryptedBytes);
    }

    /**
     * проверка активности модуля шифрования
     */
    public boolean isCryptoEnabled() {
        return !isDisableCrypto && secretKey != null;
    }
}
