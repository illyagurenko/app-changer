package ru.itone.illya4gurenko.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.itone.illya4gurenko.config.AppConfig;

import java.util.Properties;

public final class KafkaConsumerFactory {

    private KafkaConsumerFactory() {}

    public static KafkaConsumer<String, String> createConsumer(String customGroupId) {
        AppConfig config = AppConfig.getInstance();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap-servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, customGroupId != null ? customGroupId : config.get("kafka.group.id", "app-changer-group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        return new KafkaConsumer<>(props);
    }
}
