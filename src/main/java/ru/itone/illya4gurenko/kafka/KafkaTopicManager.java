package ru.itone.illya4gurenko.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.config.AppConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * автоматическое управление партициями топиков кафки
 */
@ApplicationScoped
public class KafkaTopicManager {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicManager.class);

    @Inject
    AppConfig appConfig;

    /**
     * проверяет и при необходимости увеличивает количество партиций топика
     */
    public void ensureTopicPartitions(String topicName, int targetPartitions) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get();

            if (!existingTopics.contains(topicName)) {
                // если топика нет — создаем сразу с нужным числом партиций
                NewTopic newTopic = new NewTopic(topicName, targetPartitions, (short) 1);
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                log.info("created topic {} with {} partitions", topicName, targetPartitions);
            } else {
                // если топик уже есть — проверяем число партиций
                TopicDescription desc = admin.describeTopics(Collections.singletonList(topicName))
                        .topicNameValues().get(topicName).get();

                int currentPartitions = desc.partitions().size();
                if (currentPartitions < targetPartitions) {
                    // увеличиваем количество партиций до целевого
                    admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(targetPartitions)))
                            .all().get();
                    log.info("increased partitions for {} from {} to {}", topicName, currentPartitions, targetPartitions);
                } else {
                    log.info("topic {} already has {} partitions", topicName, currentPartitions);
                }
            }
        } catch (Exception e) {
            log.warn("failed to configure partitions for topic {}: {}", topicName, e.getMessage());
        }
    }
}