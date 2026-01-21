package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.hub_event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.sensor_event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;
import net.devh.boot.grpc.server.service.GrpcService;



import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@GrpcService
@Slf4j
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private final Map<SensorEventType, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventType, HubEventHandler> hubEventHandlers;

    public EventController(List<SensorEventHandler> sensorEventHandlers, List<HubEventHandler> hubEventHandlers) {
        this.sensorEventHandlers = sensorEventHandlers.stream().collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
        this.hubEventHandlers = hubEventHandlers.stream().collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("SensorEvent grpc: {}", request);
            SensorEventProto.PayloadCase payloadCase = request.getPayloadCase();
            if (payloadCase == SensorEventProto.PayloadCase.PAYLOAD_NOT_SET) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Не задан payload события")
                        .asRuntimeException();
            }
            SensorEventHandler handler = sensorEventHandlers.get(SensorEventType.valueOf(payloadCase.name()));
            if (handler == null) {
                throw Status.UNIMPLEMENTED
                        .withDescription("Не поддерживаемый тип события датчика: " + payloadCase)
                        .asRuntimeException();
            }
            handler.handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("HubEvent grpc: {}", request);
            HubEventProto.PayloadCase payloadCase = request.getPayloadCase();
            if (payloadCase == HubEventProto.PayloadCase.PAYLOAD_NOT_SET) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Не задан payload события")
                        .asRuntimeException();
            }
            HubEventHandler handler = hubEventHandlers.get(ru.yandex.practicum.telemetry.collector.model.hub.HubEventType.valueOf(payloadCase.name()));
            if (handler == null) {
                throw Status.UNIMPLEMENTED
                        .withDescription("Не поддерживаемый тип события датчика: " + payloadCase)
                        .asRuntimeException();
            }
            handler.handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
