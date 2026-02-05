package ru.yandex.practicum.analyzer.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private final String bootstrapServers;
    private final ConsumerProperties snapshots;
    private final ConsumerProperties events;

    @Getter
    @RequiredArgsConstructor
    public static class ConsumerProperties {
        private final String topic;
        private final String groupId;
        private final String autoOffsetReset;
        private final String keyDeserializer;
        private final String valueDeserializer;
    }
}
