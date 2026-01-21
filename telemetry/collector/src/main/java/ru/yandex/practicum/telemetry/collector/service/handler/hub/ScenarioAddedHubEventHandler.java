package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.hub_event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hub_event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.hub_event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.hub_event.ScenarioConditionProto;
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
    protected ScenarioAddedEventAvro mapToAvro(HubEventProto event) {
        ScenarioAddedEventProto dto = event.getScenarioAdded();
        return ScenarioAddedEventAvro.newBuilder()
                .setName(dto.getName())
                .setActions(actionsMapToAvro(dto.getActionList()))
                .setConditions(conditionsMapToAvro(dto.getConditionList()))
                .build();
    }


    private List<DeviceActionAvro> actionsMapToAvro(List<DeviceActionProto> list) {
        List<DeviceActionAvro> result = new ArrayList<>();
        for (DeviceActionProto action : list) {
            result.add(DeviceActionAvro.newBuilder()
                    .setSensorId(action.getSensorId())
                    .setType(ActionTypeAvro.valueOf(action.getType().name()))
                    .setValue(action.getValue())
                    .build());
        }
        return result;
    }

    private List<ScenarioConditionAvro> conditionsMapToAvro(List<ScenarioConditionProto> list) {
        List<ScenarioConditionAvro> result = new ArrayList<>();

        for (ScenarioConditionProto condition : list) {
            result.add(ScenarioConditionAvro.newBuilder()
                    .setSensorId(condition.getSensorId())
                    .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                    .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                    .setValue(getValue(condition))
                    .build());
        }

        return result;
    }

    private Object getValue(ScenarioConditionProto condition) {
        return switch (condition.getValueCase()) {
            case INT_VALUE -> condition.getIntValue();
            case BOOL_VALUE -> condition.getBoolValue();
            case VALUE_NOT_SET ->
                    throw new IllegalArgumentException("Value не установлен для сенсора: " + condition.getSensorId());
        };
    }
}
