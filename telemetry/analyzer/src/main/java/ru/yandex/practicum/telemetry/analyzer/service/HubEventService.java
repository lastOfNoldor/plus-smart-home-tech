package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.telemetry.analyzer.mapper.ScenarioMapper;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.hub.*;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {
    private final ScenarioRepository scenarioRepo;
    private final ConditionRepository conditionRepo;
    private final ActionRepository actionRepo;
    private final SensorRepository sensorRepo;
    private final ScenarioMapper scenarioMapper;


    public void processEvent(HubEventAvro event) {
        HubEventType type = determineEventType(event);
        switch (type){
            case SCENARIO_ADDED :
                processScenarioAdded(event);
                break;
            case SCENARIO_REMOVED:
                processScenarioRemoved(event);
                break;
            case DEVICE_ADDED:
                processDeviceAdded(event);
                break;
            case DEVICE_REMOVED:
                processDeviceRemoved(event);
                break;
            default:
                throw new RuntimeException("Неизвестная ошибка с объектом: " + event);
        }
    }

    private HubEventType determineEventType(HubEventAvro event) {
        String type = event.getPayload().getClass().getSimpleName();
        if (type.equals(DeviceAddedEventAvro.class.getSimpleName())) {
            return HubEventType.DEVICE_ADDED;
        } else if (type.equals(ScenarioAddedEventAvro.class.getSimpleName())) {
            return HubEventType.SCENARIO_ADDED;
        } else if (type.equals(DeviceRemovedEventAvro.class.getSimpleName())) {
            return HubEventType.DEVICE_REMOVED;
        } else if (type.equals(ScenarioRemovedEventAvro.class.getSimpleName())) {
            return HubEventType.SCENARIO_REMOVED;
        }
        throw new IllegalArgumentException("Unknown payload type");
    }

    @Transactional
    private void processScenarioAdded(HubEventAvro event) {
        try {
            ScenarioAddedEventAvro avro = (ScenarioAddedEventAvro) event.getPayload();
            String hubId = event.getHubId();
            String name = avro.getName();
            Optional<Scenario> existing = scenarioRepo.findByHubIdAndName(hubId, name);
            if (existing.isPresent()) {
                Scenario oldScenario = existing.get();
                deleteScenarioWithDependencies(oldScenario);

                log.info("Удалён старый сценарий '{}' для хаба {} при обновлении",
                        name, hubId);

            }
            Scenario entity = scenarioMapper.toEntity(hubId, avro);
            scenarioRepo.save(entity);
        } catch (DataIntegrityViolationException e) {
            log.error("Ошибка целостности при сохранении сценария: {}", e.getMessage());
        }

    }

    @Transactional
    private void processDeviceRemoved(HubEventAvro event) {
        DeviceRemovedEventAvro avro = (DeviceRemovedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String id = avro.getId();
        Optional<Sensor> existing = sensorRepo.findByIdAndHubId(id, hubId);
        existing.ifPresent(sensorRepo::delete);
    }

    @Transactional
    private void processDeviceAdded(HubEventAvro event) {
        try {
            DeviceAddedEventAvro avro = (DeviceAddedEventAvro) event.getPayload();
            String hubId = event.getHubId();
            String id = avro.getId();
            if (!sensorRepo.existsByIdAndHubId(id, hubId)) {
                Sensor sensor = new Sensor(id, hubId);
                sensorRepo.save(sensor);
            }
        } catch (DataIntegrityViolationException e) {
            log.error("Ошибка целостности при сохранении сенсора: {}", e.getMessage());
        }
    }

    @Transactional
    private void processScenarioRemoved(HubEventAvro event) {
        ScenarioRemovedEventAvro avro = (ScenarioRemovedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String name = avro.getName();
        Optional<Scenario> existing = scenarioRepo.findByHubIdAndName(hubId, name);
        if (existing.isPresent()) {
            Scenario oldScenario = existing.get();
            deleteScenarioWithDependencies(oldScenario);

            log.info("Удалён старый сценарий '{}' для хаба {}",
                    name, hubId);

        }
    }

    private void deleteScenarioWithDependencies(Scenario scenario) {
        conditionRepo.deleteAll(scenario.getConditions().values());
        actionRepo.deleteAll(scenario.getActions().values());
        scenarioRepo.delete(scenario);
    }

}
