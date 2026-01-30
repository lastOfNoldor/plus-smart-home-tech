package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

public class HubEventProcessor implements Runnable {
    private final Consumer<String, HubEventAvro> hubEventConsumer;
    private final KafkaProperties props;
    private final HubEventService hubEventService;
    private final String TOPIC = props.getEvents().getTopic();


    @Override
    public void run() {
        log.info("Запуск SnapshotProcessor...");
        hubEventConsumer.subscribe(List.of(TOPIC));

        while (true) {
            ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(Duration.ofMillis(100));
            log.debug("Получено {} событий", records.count());
            for (ConsumerRecord<String, HubEventAvro> record : records) {
                log.debug("Обработка события: ключ={}, partition={}, offset={}", record.key(), record.partition(), record.offset());
                Optional<SensorsSnapshotAvro> snapshot = hubEventService.processEvent(record.value());
            }

            hubEventConsumer.commitSync();
        }
    }
}
