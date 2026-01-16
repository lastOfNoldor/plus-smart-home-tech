package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.hub.*;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import java.util.ArrayList;
import java.util.List;

@Component
public class ScenarioAddedHubEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedHubEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEvent event) {
        ScenarioAddedEvent dto = (ScenarioAddedEvent) event;
        return ScenarioAddedEventAvro.newBuilder().setName(dto.getName()).setActions(actionsMapToAvro(dto.getActions())).setConditions(conditionsMapToAvro(dto.getConditions())).build();
    }


    private List<DeviceActionAvro> actionsMapToAvro(List<DeviceAction> list) {
        List<DeviceActionAvro> result = new ArrayList<>();
        for (DeviceAction action : list) {
            result.add(DeviceActionAvro.newBuilder().setSensorId(action.getSensorId()).setType(ActionTypeAvro.valueOf(action.getType().name())).setValue(action.getValue()).build());
        }
        return result;
    }

    private List<ScenarioConditionAvro> conditionsMapToAvro(List<ScenarioCondition> list) {
        List<ScenarioConditionAvro> result = new ArrayList<>();
        for (ScenarioCondition condition : list) {
            result.add(ScenarioConditionAvro.newBuilder().setSensorId(condition.getSensorId()).setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name())).setValue(condition.getValue()).build());
        }
        return result;
    }
}
