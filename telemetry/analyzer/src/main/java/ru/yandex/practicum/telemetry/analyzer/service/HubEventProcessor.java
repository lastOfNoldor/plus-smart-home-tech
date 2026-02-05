package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final Consumer<String, HubEventAvro> hubEventConsumer;
    private final HubEventService hubEventService;
    private final KafkaProperties kafkaProperties;

    @Override
    public void run() {
        String topic = kafkaProperties.getEvents().getTopic();
        log.info("Запуск HubEventProcessor для топика: {}", topic);
        try {
            hubEventConsumer.subscribe(List.of(topic));

            while (true) {
                try {
                    ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        log.debug("Получено {} событий", records.count());
                        processRecords(records);
                    }

                } catch (Exception e) {
                    log.error("Ошибка при обработке сообщений Kafka", e);

                }
            }
        } catch (WakeupException e) {
            log.info("Получен сигнал WakeupException, начинаем graceful shutdown");
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
            }
        }

        hubEventConsumer.commitSync();
        log.debug("Зафиксированы offset для {} сообщений", records.count());
    }

    public void shutdown() {
        hubEventConsumer.wakeup();
    }
}
