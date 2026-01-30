package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {

    private final Consumer<String, SensorsSnapshotAvro> snapshotsConsumer;
    private final KafkaProperties props;
    private final String TOPIC = props.getSnapshots().getTopic();

    @Override
    public void run() {
        log.info("Запуск SnapshotProcessor...");
        snapshotsConsumer.subscribe(List.of(TOPIC));

        while (true) {
            ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotsConsumer.poll(Duration.ofMillis(100));
            log.debug("Получено {} событий", records.count());
            for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                log.debug("Обработка события: ключ={}, partition={}, offset={}",
                        record.key(), record.partition(), record.offset());
                Optional<SensorsSnapshotAvro> snapshot = aggregator.updateState(record.value());
            }

            // Ручной commit offset
            snapshotsConsumer.commitSync();
        }
    }
}
