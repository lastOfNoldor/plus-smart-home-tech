package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.yandex.practicum.telemetry.collector.model.hub.DeviceType;

@Entity
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "sensors")
public class Sensor {
    @Id
    @Column(nullable = false)
    private String id;
    @Column(nullable = false,unique = true)
    private String hubId;

}
