package ru.itone.illya4gurenko.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import ru.itone.illya4gurenko.security.AESCryptoService;

/**
 * настройки приложения из application properties
 */
@ApplicationScoped
public class AppConfig {

    @ConfigProperty(name = "kafka.bootstrap-servers", defaultValue = "localhost:9092")
    String kafkaBootstrapServers;

    @ConfigProperty(name = "kafka.group-id", defaultValue = "app-changer-group")
    String kafkaGroupId;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    @ConfigProperty(name = "app.dispatcher.url", defaultValue = "http://localhost:8085/get-datasoure-by-account")
    String dispatcherUrl;

    @ConfigProperty(name = "app.dispatcher.username", defaultValue = "APP_CHANGER")
    String dispatcherUsername;

    @ConfigProperty(name = "app.dispatcher.password", defaultValue = "xP7Llfm/e2eItNeGlxNYnQ==")
    String dispatcherPassword;

    @Inject
    AESCryptoService cryptoService;

    /**
     * заголовок basic auth для обращения к app-dispatcher
     */
    public String getDispatcherBasicAuthHeader() {
        return cryptoService.buildBasicAuthHeader(dispatcherUsername, dispatcherPassword);
    }

    /**
     * адрес брокера кафки
     */
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    /**
     * группа консьюмера по умолчанию
     */
    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    /**
     * идентификатор текущей ноды приложения
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * url адрес эндпоинта app-dispatcher
     */
    public String getDispatcherUrl() {
        return dispatcherUrl;
    }
}
