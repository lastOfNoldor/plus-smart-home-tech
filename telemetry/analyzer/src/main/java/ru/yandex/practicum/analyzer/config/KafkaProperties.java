package ru.yandex.practicum.analyzer.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@AllArgsConstructor
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private final String bootstrapServers;

    private final String snapshotsTopic;
    private final String snapshotsGroupId;
    private final String snapshotsKeyDeserializer;
    private final String snapshotsValueDeserializer;
    private final String snapshotsAutoOffsetReset;

    private final String eventsTopic;
    private final String eventsGroupId;
    private final String eventsKeyDeserializer;
    private final String eventsValueDeserializer;
    private final String eventsAutoOffsetReset;
}
