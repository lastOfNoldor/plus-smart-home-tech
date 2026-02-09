package ru.yandex.practicum.hub_router;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.hub_event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;



@Slf4j
@Service
@GrpcService
@RequiredArgsConstructor
public class HubRouterControllerStub extends HubRouterControllerGrpc.HubRouterControllerImplBase {

    // Можно внедрять другие сервисы
    // private final DeviceService deviceService;
    // private final ActionProcessor actionProcessor;

    @Override
    public void handleDeviceAction(DeviceActionRequest request,
                                   StreamObserver<Empty> responseObserver) {

        log.info("Получен DeviceActionRequest для устройства: {}", request.getAction().getSensorId());

        try {
            // Бизнес-логика
            // deviceService.processAction(request);

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.info("Успешно обработан запрос для устройства: {}", request.getAction().getSensorId());

        } catch (Exception e) {
            log.error("Ошибка обработки запроса для устройства: {}", request.getAction().getSensorId(), e);
            responseObserver.onError(e);
        }
    }
}
