package ru.yandex.practicum.telemetry.collector.model.sensor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.telemetry.collector.model.ErrorEventType;

import java.time.Instant;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", defaultImpl = ErrorEventType.class)
@JsonSubTypes({@JsonSubTypes.Type(value = ClimateSensor.class, name = "CLIMATE_SENSOR_EVENT"), @JsonSubTypes.Type(value = LightSensor.class, name = "LIGHT_SENSOR_EVENT"), @JsonSubTypes.Type(value = MotionSensor.class, name = "MOTION_SENSOR_EVENT"), @JsonSubTypes.Type(value = SwitchSensor.class, name = "SWITCH_SENSOR_EVENT"), @JsonSubTypes.Type(value = TemperatureSensor.class, name = "TEMPERATURE_SENSOR_EVENT")})
public abstract class SensorEvent {
    @NotNull
    private String id;
    @NotNull
    private String hubId;
    private Instant timestamp = Instant.now();

    @NotNull
    public abstract SensorEventType getType();
}
