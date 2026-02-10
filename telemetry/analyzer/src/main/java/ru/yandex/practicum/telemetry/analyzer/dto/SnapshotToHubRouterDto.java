package ru.yandex.practicum.telemetry.analyzer.dto;

import lombok.AllArgsConstructor;
import lombok.Value;
import ru.yandex.practicum.telemetry.collector.model.hub.ActionType;

@Value
public class SnapshotToHubRouterDto {
    String hubId;
    String name;
    String sensorId;
    ActionType type;
    Integer value;
}
