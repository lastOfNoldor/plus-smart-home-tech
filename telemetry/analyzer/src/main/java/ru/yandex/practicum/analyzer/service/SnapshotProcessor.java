package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> snapshotsConsumer;
    private final KafkaProperties props;
    private final String TOPIC = props.getSnapshots().getTopic();
    private final SnapshotService snapshotService;

    public void start() {
        log.info("Запуск SnapshotProcessor...");
        try {
            snapshotsConsumer.subscribe(List.of(TOPIC));

            while (true) {
                try {
                    ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotsConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        log.debug("Получено {} событий", records.count());
                        processSnapshots(records);
                    }
                    snapshotsConsumer.commitSync();
                } catch (Exception e) {
                    log.error("Ошибка при обработке сообщений Snapshot Kafka", e);
                }
            }
        } catch (WakeupException e) {
            log.info("Получен сигнал WakeupException, начинаем graceful shutdown");
        } finally {
            log.info("Завершение HubEventProcessor...");
            snapshotsConsumer.close();
        }
    }

    private void processSnapshots(ConsumerRecords<String, SensorsSnapshotAvro> records) {
        for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
            try {
                log.debug("Обработка события: ключ={}, partition={}, offset={}",
                        record.key(), record.partition(), record.offset());
                snapshotService.processSnapshot(record.value());
            } catch (Exception e) {
                log.error("Ошибка обработки события (partition={}, offset={}): {}",
                        record.partition(), record.offset(), e.getMessage(), e);
            }
        }

        snapshotsConsumer.commitSync();
        log.debug("Зафиксированы offset для {} сообщений", records.count());
    }

    public void shutdown() {
        snapshotsConsumer.wakeup();
    }
}
