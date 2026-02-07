package ru.yandex.practicum.telemetry.analyzer.service;


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
import ru.yandex.practicum.telemetry.collector.model.hub.ConditionType;
import org.springframework.transaction.annotation.Transactional;


import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SnapshotService {
    private final ScenarioRepository scenarioRepository;
    private final HubRouterClient hubRouterClient;

    public void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        log.info("=== НАЧАЛО ОБРАБОТКИ СНАПШОТА ===");
        log.info("Хаб: {}", hubId);
        log.info("Время снапшота: {}", snapshot.getTimestamp());
        log.info("Количество датчиков в снапшоте: {}", snapshot.getSensorsState().size());
        log.info("Найдено сценариев для хаба: {}", scenarios.size());

        // Выводим все ключи (sensorId) из снапшота
        log.info("Датчики в снапшоте: {}", snapshot.getSensorsState().keySet());

        for (Scenario scenario : scenarios) {
            log.info("--- Проверяем сценарий: '{}' ---", scenario.getName());
            log.info("   Условия сценария (датчик -> условие):");
            scenario.getConditions().forEach((sensorId, condition) -> {
                log.info("     Датчик: {}, Тип: {}, Операция: {}, Значение: {}",
                        sensorId, condition.getType(), condition.getOperation(), condition.getValue());
            });

            log.info("   Действия сценария:");
            scenario.getActions().forEach((sensorId, action) -> {
                log.info("     Датчик: {}, Действие: {}, Значение: {}",
                        sensorId, action.getType(), action.getValue());
            });

            if (checkScenarioConditions(scenario, snapshot)) {
                log.info("✅ Сценарий '{}' ВЫПОЛНЕН!", scenario.getName());
                sendScenarioActions(scenario, hubId);
            } else {
                log.info("❌ Сценарий '{}' НЕ выполнен", scenario.getName());
            }
        }
        log.info("=== КОНЕЦ ОБРАБОТКИ СНАПШОТА ===");
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        Map<String, Condition> conditions = scenario.getConditions();
        log.debug("Проверяем {} условий для сценария '{}'",
                conditions.size(), scenario.getName());

        for (Map.Entry<String, Condition> entry : conditions.entrySet()) {
            String sensorId = entry.getKey();
            Condition condition = entry.getValue();

            log.debug("Проверяем условие для датчика {}: {} {} {}",
                    sensorId, condition.getType(), condition.getOperation(), condition.getValue());

            if (!isConditionMet(sensorId, condition, snapshot)) {
                log.debug("Условие для датчика {} НЕ выполнено", sensorId);
                return false;
            }
            log.debug("Условие для датчика {} ВЫПОЛНЕНО", sensorId);
        }

        log.debug("Все условия сценария '{}' выполнены", scenario.getName());
        return true;
    }

    private boolean isConditionMet(String sensorId, Condition condition, SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> sensorsStates = snapshot.getSensorsState();
        SensorStateAvro sensorStateAvro = sensorsStates.get(sensorId);

        if (sensorStateAvro != null) {
            log.debug("Датчик {} найден в снапшоте, тип данных: {}",
                    sensorId, sensorStateAvro.getData().getClass().getSimpleName());
            boolean result = checkCondition(sensorStateAvro, condition);
            log.debug("Результат проверки датчика {}: {}", sensorId, result);
            return result;
        } else {
            log.warn("⚠️ Датчик {} ОТСУТСТВУЕТ в снапшоте!", sensorId);
            log.warn("   Доступные датчики в снапшоте: {}", sensorsStates.keySet());
            return false;
        }
    }

    private void sendScenarioActions(Scenario scenario, String hubId) {
        Map<String, Action> actions = scenario.getActions();
        log.info("Отправка {} действий для сценария '{}'", actions.size(), scenario.getName());

        for (Map.Entry<String, Action> actionEntry : actions.entrySet()) {
            String sensorId = actionEntry.getKey();
            Action action = actionEntry.getValue();

            log.info("   Действие для датчика {}: {} со значением {}",
                    sensorId, action.getType(), action.getValue());

            SnapshotToHubRouterDto hubRouterClientMessage = toHubRouterClient(hubId, scenario, sensorId, action);

            try {
                hubRouterClient.sendToHubRouter(hubRouterClientMessage);
                log.info("✅ Действие для датчика {} успешно отправлено", sensorId);
            } catch (Exception e) {
                log.error("❌ Ошибка отправки действия для датчика {}: {}", sensorId, e.getMessage());
            }
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

        log.debug("Проверка условия: тип={}, данные класса={}",
                type, data.getClass().getSimpleName());

        if (sensorValue == null) {
            log.warn("⚠️ Не удалось извлечь значение для типа {} из данных класса {}",
                    type, data.getClass().getSimpleName());
            return false;
        }

        boolean result = switch (condition.getOperation()) {
            case EQUALS -> {
                log.debug("Сравнение EQUALS: датчик={}, условие={}",
                        sensorValue, condition.getValue());
                yield sensorValue.equals(condition.getValue());
            }
            case GREATER_THAN -> {
                log.debug("Сравнение GREATER_THAN: датчик={}, условие={}",
                        sensorValue, condition.getValue());
                yield sensorValue > condition.getValue();
            }
            case LOWER_THAN -> {
                log.debug("Сравнение LOWER_THAN: датчик={}, условие={}",
                        sensorValue, condition.getValue());
                yield sensorValue < condition.getValue();
            }
        };

        log.info("Условие: {} {} {}, значение датчика: {} -> {}",
                condition.getType(), condition.getOperation(),
                condition.getValue(), sensorValue, result);

        return result;
    }

    private Integer extractValue(ConditionType type, Object data) {
        log.debug("Извлечение значения для типа {} из данных класса {}",
                type, data.getClass().getSimpleName());

        return switch (type) {
            case MOTION -> {
                if (data.getClass().equals(MotionSensorAvro.class)) {
                    boolean motion = ((MotionSensorAvro) data).getMotion();
                    log.debug("MotionSensor: motion={} -> {}", motion, motion ? 1 : 0);
                    yield motion ? 1 : 0;
                }
                log.warn("⚠️ Ожидался MotionSensorAvro, но получен {}", data.getClass());
                yield null;
            }
            case LUMINOSITY -> {
                if (data.getClass().equals(LightSensorAvro.class)) {
                    int luminosity = ((LightSensorAvro) data).getLuminosity();
                    log.debug("LightSensor: luminosity={}", luminosity);
                    yield luminosity;
                }
                log.warn("⚠️ Ожидался LightSensorAvro, но получен {}", data.getClass());
                yield null;
            }
            case SWITCH -> {
                if (data.getClass().equals(SwitchSensorAvro.class)) {
                    boolean state = ((SwitchSensorAvro) data).getState();
                    log.debug("SwitchSensor: state={} -> {}", state, state ? 1 : 0);
                    yield state ? 1 : 0;
                }
                log.warn("⚠️ Ожидался SwitchSensorAvro, но получен {}", data.getClass());
                yield null;
            }
            case TEMPERATURE -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    int temp = ((ClimateSensorAvro) data).getTemperatureC();
                    log.debug("ClimateSensor: temperatureC={}", temp);
                    yield temp;
                } else if (data.getClass().equals(TemperatureSensorAvro.class)) {
                    int temp = ((TemperatureSensorAvro) data).getTemperatureC();
                    log.debug("TemperatureSensor: temperatureC={}", temp);
                    yield temp;
                }
                log.warn("⚠️ Ожидался ClimateSensorAvro или TemperatureSensorAvro, но получен {}", data.getClass());
                yield null;
            }
            case CO2LEVEL -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    int co2 = ((ClimateSensorAvro) data).getCo2Level();
                    log.debug("ClimateSensor: co2Level={}", co2);
                    yield co2;
                }
                log.warn("⚠️ Ожидался ClimateSensorAvro, но получен {}", data.getClass());
                yield null;
            }
            case HUMIDITY -> {
                if (data.getClass().equals(ClimateSensorAvro.class)) {
                    int humidity = ((ClimateSensorAvro) data).getHumidity();
                    log.debug("ClimateSensor: humidity={}", humidity);
                    yield humidity;
                }
                log.warn("⚠️ Ожидался ClimateSensorAvro, но получен {}", data.getClass());
                yield null;
            }
        };
    }
}