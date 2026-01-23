package ru.yandex.practicum.telemetry.collector.util;

import io.grpc.Status;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.grpc.telemetry.hub_event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.sensor_event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

@Component
public class GrpcRequestValidator {

    public void validateSensorPayload(SensorEventProto.PayloadCase payloadCase) {
        if (payloadCase == SensorEventProto.PayloadCase.PAYLOAD_NOT_SET) {
            throw Status.INVALID_ARGUMENT.withDescription("Не задан payload события датчика").asRuntimeException();
        }
    }

    public void validateHubPayload(HubEventProto.PayloadCase payloadCase) {
        if (payloadCase == HubEventProto.PayloadCase.PAYLOAD_NOT_SET) {
            throw Status.INVALID_ARGUMENT.withDescription("Не задан payload события хаба").asRuntimeException();
        }
    }

    public void validateSensorHandler(SensorEventHandler handler, SensorEventProto.PayloadCase payloadCase) {
        if (handler == null) {
            SensorEventType type = SensorEventType.valueOf(payloadCase.name());
            throw Status.UNIMPLEMENTED.withDescription("Не поддерживаемый тип события датчика: " + type).asRuntimeException();
        }
    }

    public void validateHubHandler(HubEventHandler handler, HubEventProto.PayloadCase payloadCase) {
        if (handler == null) {
            HubEventType type = HubEventType.valueOf(payloadCase.name());
            throw Status.UNIMPLEMENTED.withDescription("Не поддерживаемый тип события хаба: " + type).asRuntimeException();
        }
    }
}
