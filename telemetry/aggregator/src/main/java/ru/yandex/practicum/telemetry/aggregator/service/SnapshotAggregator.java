package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class SnapshotAggregator {
    Map<String, SensorsSnapshotAvro> storage = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String deviceId = event.getId();
        SensorsSnapshotAvro currentSnapshot = storage.get(hubId);
        if (currentSnapshot == null) {
            return addNewSnapshot(event);
        }

        Map<String, SensorStateAvro> sensorsState = new HashMap<>(currentSnapshot.getSensorsState());
        SensorStateAvro existingState = sensorsState.get(deviceId);
        if (existingState == null) {
            addNewState(event, sensorsState);
        } else {
            if (validateUpdatedState(event, existingState)) {
                return Optional.empty();
            }
            updateSensorState(event, sensorsState);
        }
        return updateSnapshot(event, sensorsState);
    }

    private Optional<SensorsSnapshotAvro> updateSnapshot(SensorEventAvro event, Map<String, SensorStateAvro> sensorsState) {
        String hubId = event.getHubId();
        Instant eventTimestamp = event.getTimestamp();
        SensorsSnapshotAvro updatedSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(eventTimestamp)
                .setSensorsState(sensorsState)
                .build();
        storage.put(hubId, updatedSnapshot);
        return Optional.of(updatedSnapshot);
    }


    private Optional<SensorsSnapshotAvro> addNewSnapshot(SensorEventAvro event) {
        String hubId = event.getHubId();
        String deviceId = event.getId();
        Instant eventTimestamp = event.getTimestamp();

        Map<String, SensorStateAvro> sensorsState = new HashMap<>();
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();
        sensorsState.put(deviceId, newState);

        SensorsSnapshotAvro newSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(eventTimestamp)
                .setSensorsState(sensorsState)
                .build();

        storage.put(hubId, newSnapshot);
        return Optional.of(newSnapshot);

    }

    private boolean validateUpdatedState(SensorEventAvro event, SensorStateAvro existingState) {
        Instant eventTimestamp = event.getTimestamp();
        if (eventTimestamp.isBefore(existingState.getTimestamp())) {
            return true;
        }
        if (eventTimestamp.equals(existingState.getTimestamp())
                && event.getPayload().equals(existingState.getData())) {
            return true;
        }
        return false;
    }

    private void updateSensorState (SensorEventAvro event, Map<String, SensorStateAvro> sensorsState) {
        String deviceId = event.getId();
        Instant eventTimestamp = event.getTimestamp();

        SensorStateAvro updatedState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();

        sensorsState.put(deviceId, updatedState);
    }


    private void addNewState (SensorEventAvro event, Map<String, SensorStateAvro> sensorsState) {
        Instant eventTimestamp = event.getTimestamp();
        String deviceId = event.getId();
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();
        sensorsState.put(deviceId, newState);
    }


}
