package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Component
public class ScenarioRemovedHubEventHandler extends BaseHubEventHandler<ScenarioRemovedEventAvro> {

    public ScenarioRemovedHubEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_REMOVED;
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvro(HubEvent event) {
        ScenarioRemovedEvent dto = (ScenarioRemovedEvent) event;

        return ScenarioRemovedEventAvro.newBuilder().setName(dto.getName()).build();
    }
}
