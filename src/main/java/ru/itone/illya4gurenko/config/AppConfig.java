package ru.itone.illya4gurenko.config;


import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final AppConfig INSTANCE = new AppConfig();
    private final Properties properties = new Properties();

    private AppConfig() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                properties.load(is);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to load application.properties", e);
        }
    }

    public static AppConfig getInstance() {
        return INSTANCE;
    }

    public String get(String key, String def) {
        String envKey = key.replace('.', '_').toUpperCase();
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) return envVal;
        return properties.getProperty(key, def);
    }

    public String getDbUrl() { return get("db.url", "jdbc:postgresql://localhost:5432/changer_db"); }
    public String getDbUsername() { return get("db.username", "changer_user"); }
    public String getDbPassword() { return get("db.password", "root"); }
    public int getDbPoolSize() { return Integer.parseInt(get("db.pool.size", "10")); }
    public String getNodeId() { return get("app.node.id", "162"); }
}
