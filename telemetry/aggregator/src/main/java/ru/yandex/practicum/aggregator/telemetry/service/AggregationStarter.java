package ru.yandex.practicum.aggregator.telemetry.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SensorsSnapshotAvro> producer;
    private final SnapshotAggregator aggregator;

    public void start() {



        // Подписка на топик telemetry.sensors.v1
        // Основной цикл poll()
        // Обработка каждого события через aggregator.updateState()
        // Отправка обновленного снапшота в telemetry.snapshots.v1
        // Обработка shutdown
    }
}
