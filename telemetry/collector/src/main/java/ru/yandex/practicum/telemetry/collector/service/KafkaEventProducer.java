package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Slf4j
public class KafkaEventProducer {

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, SpecificRecordBase> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void send(SpecificRecordBase event, String key, Instant timestamp, String topicName) {
        try {
            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(topicName, null, timestamp.toEpochMilli(), key, event);
            kafkaTemplate.send(record);
            log.debug("Сообщение отправлено в: {}, ключ: {}", topicName, key);
        } catch (Exception e) {
            log.error("Ошибка отправки сообщения в Kafka. Topic: {}, ключ: {}", topicName, key, e);
        }
    }
}
