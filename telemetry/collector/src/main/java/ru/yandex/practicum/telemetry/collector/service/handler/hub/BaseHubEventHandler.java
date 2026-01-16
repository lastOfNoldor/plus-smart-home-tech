package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

@Slf4j
@Component
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {
    private static final String HUB_EVENTS_TOPIC = "telemetry.hubs.v1";
    protected final KafkaEventProducer producer;

    public BaseHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract T mapToAvro(HubEvent event);

    @Override
    public void handle(HubEvent event) {
        if (!event.getType().equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getType());
        }
        T payload = mapToAvro(event);

        HubEventAvro eventAvro = HubEventAvro.newBuilder().setHubId(event.getHubId()).setTimestamp(event.getTimestamp()).setPayload(payload).build();

        producer.send(eventAvro, event.getHubId(), event.getTimestamp(), HUB_EVENTS_TOPIC);
        log.info("отправленное Hub event сообщение: {}", eventAvro);
    }
}
