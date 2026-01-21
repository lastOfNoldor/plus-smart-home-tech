package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.hub_event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

import java.time.Instant;

@Slf4j
@Component
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {
    private static final String HUB_EVENTS_TOPIC = "telemetry.hubs.v1";
    protected final KafkaEventProducer producer;

    public BaseHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract T mapToAvro(HubEventProto event);

    @Override
    public void handle(HubEventProto proto) {
        HubEventType type = HubEventType.valueOf(proto.getPayloadCase().name());
        if (!type.equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + type);
        }
        T payload = mapToAvro(proto);
        Instant time = convertTimestamp(proto.getTimestamp());
        HubEventAvro eventAvro = HubEventAvro.newBuilder().setHubId(proto.getHubId()).setTimestamp(time).setPayload(payload).build();

        producer.send(eventAvro, proto.getHubId(), time, HUB_EVENTS_TOPIC);
        log.info("отправленное Hub proto сообщение: {}", eventAvro);
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
