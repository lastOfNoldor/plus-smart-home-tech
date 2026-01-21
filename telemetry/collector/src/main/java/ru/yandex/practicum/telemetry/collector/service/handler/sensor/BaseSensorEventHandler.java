package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.sensor_event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

import java.time.Instant;

@Slf4j
@Component
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {
    private static final String SENSOR_EVENTS_TOPIC = "telemetry.sensors.v1";
    protected final KafkaEventProducer producer;

    public BaseSensorEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public void handle(SensorEventProto proto) {
        SensorEventType type = SensorEventType.valueOf(proto.getPayloadCase().name());
        if (!type.equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + type);
        }

        T payload = mapToAvro(proto);
        Instant time = convertTimestamp(proto.getTimestamp());
        SensorEventAvro eventAvro = SensorEventAvro.newBuilder().setId(proto.getId()).setHubId(proto.getHubId()).setTimestamp(time).setPayload(payload).build();

        producer.send(eventAvro, proto.getHubId(), time, SENSOR_EVENTS_TOPIC);
        log.info("отправленное Sensor event сообщение: {}", eventAvro);
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

}