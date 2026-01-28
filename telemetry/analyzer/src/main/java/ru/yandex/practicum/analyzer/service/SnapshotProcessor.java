package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaProperties;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {

    private final KafkaConsumer<String, Object> snapshotsConsumer;
    private final KafkaProperties props;

    @Override
    public void run() {
        // Подписываемся на топик
        snapshotsConsumer.subscribe(List.of(props.getSnapshotsTopic()));

        while (true) {
            ConsumerRecords<String, Object> records = snapshotsConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Object> record : records) {
                // Обработка снапшота
            }

            // Ручной commit offset
            snapshotsConsumer.commitSync();
        }
    }
}
