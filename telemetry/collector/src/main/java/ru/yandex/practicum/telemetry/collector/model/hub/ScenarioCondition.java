package ru.yandex.practicum.telemetry.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScenarioCondition {
    @NotNull
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Integer intValue;
    private Boolean boolValue;

    public Object getValue() {
        if (intValue != null) {
            return intValue;
        } else if (boolValue != null) {
            return boolValue;
        }
        return null;
    }

    public void setValue(Object value) {
        if (value == null) {
            this.intValue = null;
            this.boolValue = null;
        } else if (value instanceof Integer) {
            this.intValue = (Integer) value;
            this.boolValue = null;
        } else if (value instanceof Boolean) {
            this.boolValue = (Boolean) value;
            this.intValue = null;
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }
}
