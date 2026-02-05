package ru.yandex.practicum.analyzer.client;

import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.dto.SnapshotToHubRouterDto;
import ru.yandex.practicum.grpc.telemetry.hub_event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.hub_event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hub_event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.telemetry.collector.model.hub.ActionType;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubRouterClient {

    @GrpcClient("hub-router")
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub;

    public void sendToHubRouter(SnapshotToHubRouterDto hubRouterClientMessage) {
        String hubId = hubRouterClientMessage.getHubId();
        String name = hubRouterClientMessage.getName();
        String sensorId = hubRouterClientMessage.getSensorId();
        try {
            log.info("отправка в Hub Router..");
            DeviceActionRequest request = DeviceActionRequest.newBuilder().setHubId(hubId)
                    .setScenarioName(name)
                    .setAction(setDeviceAction(hubRouterClientMessage))
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(Instant.now().getEpochSecond())
                            .setNanos(Instant.now().getNano()))
                    .build();
            hubRouterStub.handleDeviceAction(request);
            log.debug("Отправлено gRPC действие: hub={}, scenario={}, sensor={}",
                    hubId, name, sensorId);
        } catch (StatusRuntimeException e) {
            log.error("gRPC ошибка для хаба {}: {}", hubId, e.getStatus(), e);
            throw new RuntimeException("gRPC call failed: " + e.getStatus(), e);
        }
    }

    private DeviceActionProto setDeviceAction(SnapshotToHubRouterDto hubRouterClientMessage) {
        String sensorId = hubRouterClientMessage.getSensorId();
        ActionType type = hubRouterClientMessage.getType();
        Integer value = hubRouterClientMessage.getValue();
        return DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(type.name()))
                .setValue(value != null ? value : 0)
                .build();
    }
}
