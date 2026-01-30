package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.mapper.ScenarioMapper;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.*;

import java.util.Optional;

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
        if (type.equals(DeviceAddedEvent.class.getSimpleName())) {
            return HubEventType.DEVICE_ADDED;
        } else if (type.equals(ScenarioAddedEvent.class.getSimpleName())) {
            return HubEventType.SCENARIO_ADDED;
        } else if (type.equals(DeviceRemovedEvent.class.getSimpleName())) {
            return HubEventType.DEVICE_REMOVED;
        } else if (type.equals(ScenarioRemovedEvent.class.getSimpleName())) {
            return HubEventType.SCENARIO_REMOVED;
        }
        throw new IllegalArgumentException("Unknown payload type");
    }

    @Transactional
    private void processScenarioAdded(HubEventAvro event) {
        ScenarioAddedEventAvro avro = (ScenarioAddedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String name = avro.getName();
        Optional<Scenario> possibleScenario = scenarioRepo.findByHubIdAndName(hubId, name);
        if (possibleScenario.isPresent()) {
            scenarioRepo.deleteByHubIdAndName(hubId,name);
        } else {
            Scenario entity = scenarioMapper.toEntity(hubId, avro);
            scenarioRepo.save(entity);
        }

    }

    private void processDeviceRemoved(HubEventAvro event) {


    }

    private void processDeviceAdded(HubEventAvro event) {
    }

    private void processScenarioRemoved(HubEventAvro event) {
    }



}
