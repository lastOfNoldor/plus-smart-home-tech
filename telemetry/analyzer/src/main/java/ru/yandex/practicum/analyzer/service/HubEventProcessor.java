package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor

public class HubEventProcessor implements Runnable {
    private final Consumer<String, SensorEventAvro> consumer;


    @Override
    public void run() {
        // подписка на топики
        // ...
        // цикл опроса
    }

    // ...детали реализации...
}
