package ru.yandex.practicum.telemetry.analyzer.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.analyzer.client.HubRouterClient;
import ru.yandex.practicum.telemetry.analyzer.dto.SnapshotToHubRouterDto;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.hub.ActionType;
import ru.yandex.practicum.telemetry.collector.model.hub.ConditionType;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService {
    private final ScenarioRepository scenarioRepository;
    private final HubRouterClient hubRouterClient;


    public void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        for (Scenario scenario : scenarios) {
            if (checkScenarioConditions(scenario, snapshot)) {
                sendScenarioActions(scenario, hubId);
            }
        }
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        Map<String, Condition> conditions = scenario.getConditions();

        for (Map.Entry<String, Condition> entry : conditions.entrySet()) {
            if (!isConditionMet(entry.getKey(), entry.getValue(), snapshot)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConditionMet(String sensorId, Condition condition, SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> sensorsStates = snapshot.getSensorsState();
        SensorStateAvro sensorStateAvro = sensorsStates.get(sensorId);

        if (sensorStateAvro != null) {
            return checkCondition(sensorStateAvro, condition);
        } else {
            log.debug("Датчик {} отсутствует в снапшоте", sensorId);
            return false;
        }
    }

    private void sendScenarioActions(Scenario scenario, String hubId) {
        Map<String, Action> actions = scenario.getActions();
        for (Map.Entry<String, Action> actionEntry : actions.entrySet()) {
            String sensorId = actionEntry.getKey();
            Action action = actionEntry.getValue();
            SnapshotToHubRouterDto hubRouterClientMessage = toHubRouterClient(hubId, scenario, sensorId, action);
            hubRouterClient.sendToHubRouter(hubRouterClientMessage);
        }
        log.info("Сценарий '{}' выполнен, отправлено {} действий",
                scenario.getName(), actions.size());
    }

    private SnapshotToHubRouterDto toHubRouterClient(String hubId, Scenario scenario, String sensorId, Action action) {
        return new SnapshotToHubRouterDto(hubId,
                scenario.getName(),
                sensorId,
                action.getType(),
                action.getValue());
    }

    private boolean checkCondition(SensorStateAvro sensorStateAvro, Condition condition) {
        Object data = sensorStateAvro.getData();
        ConditionType type = condition.getType();
        Integer sensorValue = extractValue(type, data);
        if (sensorValue == null) return false;

        boolean result = switch (condition.getOperation()) {
            case EQUALS -> sensorValue.equals(condition.getValue());
            case GREATER_THAN -> sensorValue > condition.getValue();
            case LOWER_THAN -> sensorValue < condition.getValue();
        };

        log.debug("Условие: {} {} {}, значение датчика: {} -> {}",
                condition.getType(), condition.getOperation(),
                condition.getValue(), sensorValue, result);

        return result;
    }

    private Integer extractValue(ConditionType type, Object data) {
        return switch (type) {
            case MOTION -> {
                if (data.getClass().equals(MotionSensorAvro.class)) {
                    yield ((MotionSensorAvro) data).getMotion() ? 1 : 0;
                }
                yield null;
            }
            case LUMINOSITY -> {
                if (data.getClass().equals(LightSensorAvro.class)) {
                    yield ((LightSensorAvro) data).getLuminosity();
                }
                yield null;
            }
            case SWITCH -> {
                if (data.getClass().equals(SwitchSensorAvro.class)) {
                    yield ((SwitchSensorAvro) data).getState() ? 1 : 0;
                }
                yield null;
            }
            case TEMPERATURE -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    yield ((ClimateSensorAvro) data).getTemperatureC();
                } else if (data.getClass().equals(TemperatureSensorAvro.class)) {
                    yield ((TemperatureSensorAvro) data).getTemperatureC();
                }
                yield null;
            }
            case CO2LEVEL -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    yield ((ClimateSensorAvro) data).getCo2Level();
                }
                yield null;
            }
            case HUMIDITY -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    yield ((ClimateSensorAvro) data).getHumidity();
                }
                yield null;
            }
        };
    }

}
