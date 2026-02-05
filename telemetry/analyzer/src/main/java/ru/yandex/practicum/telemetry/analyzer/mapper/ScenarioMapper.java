package ru.yandex.practicum.telemetry.analyzer.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.hub.ActionType;
import ru.yandex.practicum.telemetry.collector.model.hub.ConditionOperation;
import ru.yandex.practicum.telemetry.collector.model.hub.ConditionType;


import java.util.HashMap;
import java.util.Map;


@Component
public class ScenarioMapper {

    public Scenario toEntity(String hubId, ScenarioAddedEventAvro avroEvent) {
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(avroEvent.getName());

        Map<String, Condition> conditions = new HashMap<>();
        for (ScenarioConditionAvro avroCond : avroEvent.getConditions()) {
            Condition condition = new Condition();
            condition.setType(mapConditionType(avroCond.getType()));
            condition.setOperation(mapConditionOperation(avroCond.getOperation()));
            condition.setValue(mapValue(avroCond));

            conditions.put(avroCond.getSensorId(), condition);
        }
        scenario.setConditions(conditions);

        // Маппинг действий
        Map<String, Action> actions = new HashMap<>();
        for (DeviceActionAvro avroAction : avroEvent.getActions()) {
            Action action = new Action();
            action.setType(mapActionType(avroAction.getType()));
            action.setValue(avroAction.getValue());

            actions.put(avroAction.getSensorId(), action);
        }
        scenario.setActions(actions);

        return scenario;
    }

    private ConditionType mapConditionType(ConditionTypeAvro avroType) {
        return ConditionType.valueOf(avroType.name());
    }

    private ConditionOperation mapConditionOperation(ConditionOperationAvro avroOperation) {
        return ConditionOperation.valueOf(avroOperation.name());
    }

    private ActionType mapActionType(ActionTypeAvro avroType) {
        return ActionType.valueOf(avroType.name());
    }

    private Integer mapValue(Object avroValue) {
        return switch (avroValue) {
            case null -> null;
            case Integer integer -> integer;
            case Boolean b -> Boolean.TRUE.equals(avroValue) ? 1 : 0;
            default -> throw new IllegalArgumentException("Unsupported value type: " + avroValue.getClass());
        };
    }
}
