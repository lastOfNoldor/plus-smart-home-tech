package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

@Slf4j
@Component
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {
    private static final String SENSOR_EVENTS_TOPIC = "telemetry.sensors.v1";
    protected final KafkaEventProducer producer;

    public BaseSensorEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract T mapToAvro(SensorEvent event);

    @Override
    public void handle(SensorEvent event) {
        if (!event.getType().equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getType());
        }
        T payload = mapToAvro(event);

        SensorEventAvro eventAvro = SensorEventAvro.newBuilder().setId(event.getId()).setHubId(event.getHubId()).setTimestamp(event.getTimestamp()).setPayload(payload).build();

        producer.send(eventAvro, event.getHubId(), event.getTimestamp(), SENSOR_EVENTS_TOPIC);
        log.info("отправленное Sensor event сообщение: {}", eventAvro);
    }
}