package ru.itone.illya4gurenko.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.itone.illya4gurenko.config.AppConfig;

import java.util.Properties;

/**
 * фабрика для создания kafka консьюмеров
 */
@ApplicationScoped
public class KafkaConsumerFactory {

    @Inject
    AppConfig appConfig;

    /**
     * создает ручной консьюмер с указанным или дефолтным group id
     */
    public KafkaConsumer<String, String> createConsumer(String customGroupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, customGroupId != null ? customGroupId : appConfig.getKafkaGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        return new KafkaConsumer<>(props);
    }
}