package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;


@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SensorsSnapshotAvro> producer;
    private final SnapshotAggregator aggregator;
    @Value("${spring.kafka.topics.consumer}")
    private String consumerTopic;
    @Value("${spring.kafka.topics.producer}")
    private String producerTopic;

    public void start() {
        try {
            log.info("Запуск AggregationStarter...");
            consumer.subscribe(List.of(consumerTopic));
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofSeconds(10));
                log.debug("Получено {} событий", records.count());
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    log.debug("Обработка события: ключ={}, partition={}, offset={}",
                            record.key(), record.partition(), record.offset());
                    Optional<SensorsSnapshotAvro> snapshot = aggregator.updateState(record.value());
                    if (snapshot.isPresent()) {
                        ProducerRecord<String, SensorsSnapshotAvro> producerRecord = new ProducerRecord<>(producerTopic, snapshot.get().getHubId(), snapshot.get());
                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Не удалось отправить snapshot {}", exception.toString());
                                throw new RuntimeException("Failed to send snapshot", exception);
                            }
                        });
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    log.debug("Оффсеты зафиксированы");
                }
            }
        } catch (WakeupException e) {
            log.info("Получен сигнал WakeupException, начинаем graceful shutdown");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            producer.flush();
            consumer.close();
            producer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
