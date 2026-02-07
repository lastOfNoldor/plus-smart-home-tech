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
        // ВРЕМЕННОЕ ЛОГИРОВАНИЕ ДЛЯ ТЕСТОВ
        System.out.println("=== ANALYZER DEBUG LOG ===");
        System.out.println("Processing snapshot for hub: " + snapshot.getHubId());
        System.out.println("Timestamp: " + snapshot.getTimestamp());
        System.out.println("Sensors in snapshot: " + snapshot.getSensorsState().keySet());

        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        System.out.println("Found " + scenarios.size() + " scenarios for hub " + hubId);

        for (Scenario scenario : scenarios) {
            System.out.println("\n--- Checking scenario: " + scenario.getName() + " ---");
            System.out.println("Conditions: " + scenario.getConditions());

            if (checkScenarioConditions(scenario, snapshot)) {
                System.out.println("✅ Scenario '" + scenario.getName() + "' conditions MET!");
                sendScenarioActions(scenario, hubId);
            } else {
                System.out.println("❌ Scenario '" + scenario.getName() + "' conditions NOT met");
            }
        }
        System.out.println("=== END DEBUG LOG ===\n");
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        Map<String, Condition> conditions = scenario.getConditions();

        for (Map.Entry<String, Condition> entry : conditions.entrySet()) {
            String sensorId = entry.getKey();
            Condition condition = entry.getValue();

            System.out.println("\nChecking condition for sensor: " + sensorId);
            System.out.println("Condition type: " + condition.getType());
            System.out.println("Condition operation: " + condition.getOperation());
            System.out.println("Condition value: " + condition.getValue());

            Map<String, SensorStateAvro> sensorsStates = snapshot.getSensorsState();
            SensorStateAvro sensorStateAvro = sensorsStates.get(sensorId);

            if (sensorStateAvro == null) {
                System.out.println("⚠️ Sensor " + sensorId + " NOT FOUND in snapshot!");
                System.out.println("Available sensors: " + sensorsStates.keySet());
                return false;
            }

            Object data = sensorStateAvro.getData();
            System.out.println("Sensor data class: " + (data != null ? data.getClass().getName() : "null"));
            System.out.println("Sensor data: " + data);

            Integer sensorValue = extractValue(condition.getType(), data);
            System.out.println("Extracted value: " + sensorValue);

            if (sensorValue == null) {
                System.out.println("❌ Could not extract value for type: " + condition.getType());
                return false;
            }

            boolean conditionMet = switch (condition.getOperation()) {
                case EQUALS -> sensorValue.equals(condition.getValue());
                case GREATER_THAN -> sensorValue > condition.getValue();
                case LOWER_THAN -> sensorValue < condition.getValue();
            };

            System.out.println("Condition result: " + sensorValue + " " +
                    condition.getOperation() + " " + condition.getValue() + " = " + conditionMet);

            if (!conditionMet) {
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
        // ДОБАВЬТЕ ЛОГИРОВАНИЕ СЮДА ТОЖЕ
        System.out.println("DEBUG extractValue - type: " + type + ", data class: " + (data != null ? data.getClass().getSimpleName() : "null"));

        return switch (type) {
            case MOTION -> {
                if (data instanceof MotionSensorAvro motionSensor) {
                    int value = motionSensor.getMotion() ? 1 : 0;
                    System.out.println("DEBUG: Motion sensor value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a MotionSensorAvro");
                yield null;
            }
            case LUMINOSITY -> {
                if (data instanceof LightSensorAvro lightSensor) {
                    int value = lightSensor.getLuminosity();
                    System.out.println("DEBUG: Light sensor value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a LightSensorAvro");
                yield null;
            }
            case SWITCH -> {
                if (data instanceof SwitchSensorAvro switchSensor) {
                    int value = switchSensor.getState() ? 1 : 0;
                    System.out.println("DEBUG: Switch sensor value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a SwitchSensorAvro");
                yield null;
            }
            case TEMPERATURE -> {
                if (data instanceof TemperatureSensorAvro tempSensor) {
                    int value = tempSensor.getTemperatureC();
                    System.out.println("DEBUG: Temperature sensor (simple) value = " + value);
                    yield value;
                } else if (data instanceof ClimateSensorAvro climateSensor) {
                    int value = climateSensor.getTemperatureC();
                    System.out.println("DEBUG: Climate sensor temperature value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a temperature sensor");
                yield null;
            }
            case CO2LEVEL -> {
                if (data instanceof ClimateSensorAvro climateSensor) {
                    int value = climateSensor.getCo2Level();
                    System.out.println("DEBUG: CO2 level value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a ClimateSensorAvro");
                yield null;
            }
            case HUMIDITY -> {
                if (data instanceof ClimateSensorAvro climateSensor) {
                    int value = climateSensor.getHumidity();
                    System.out.println("DEBUG: Humidity value = " + value);
                    yield value;
                }
                System.out.println("DEBUG: Not a ClimateSensorAvro");
                yield null;
            }
        };
    }
}
