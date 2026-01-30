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

@Component
@Slf4j
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final Consumer<String, HubEventAvro> hubEventConsumer;
    private final HubEventService hubEventService;
    private final KafkaProperties kafkaProperties;

    @Override
    public void run() {
        String topic = kafkaProperties.getEvents().getTopic(); // или kafkaProperties.getEvents().getTopic()
        log.info("Запуск HubEventProcessor для топика: {}", topic);

        try {
            hubEventConsumer.subscribe(List.of(topic));

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        log.debug("Получено {} событий", records.count());
                        processRecords(records);
                    }

                } catch (Exception e) {
                    log.error("Ошибка при обработке сообщений Kafka", e);
                    // Можно добавить паузу перед повторной попыткой
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            log.info("HubEventProcessor прерван");
            Thread.currentThread().interrupt();
        } finally {
            log.info("Завершение HubEventProcessor...");
            hubEventConsumer.close();
        }
    }

    private void processRecords(ConsumerRecords<String, HubEventAvro> records) {
        for (ConsumerRecord<String, HubEventAvro> record : records) {
            try {
                log.debug("Обработка события: ключ={}, partition={}, offset={}",
                        record.key(), record.partition(), record.offset());

                hubEventService.processEvent(record.value());

            } catch (Exception e) {
                log.error("Ошибка обработки события (partition={}, offset={}): {}",
                        record.partition(), record.offset(), e.getMessage(), e);
                // Решение: пропустить и продолжить или отправить в DLQ
            }
        }

        // Фиксация offset после успешной обработки ВСЕХ записей
        hubEventConsumer.commitSync();
        log.debug("Зафиксированы offset для {} сообщений", records.count());
    }
}
