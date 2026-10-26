package ru.itone.illya4gurenko.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class AppConfig {

    @ConfigProperty(name = "kafka.bootstrap-servers", defaultValue = "localhost:9092")
    String kafkaBootstrapServers;

    @ConfigProperty(name = "kafka.group-id", defaultValue = "app-changer-group")
    String kafkaGroupId;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getNodeId() {
        return nodeId;
    }
}