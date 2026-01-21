package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.sensor_event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.sensor_event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Component
public class MotionSensorEventHandler extends BaseSensorEventHandler<MotionSensorAvro> {

    public MotionSensorEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.MOTION_SENSOR;
    }

    @Override
    protected MotionSensorAvro mapToAvro(SensorEventProto proto) {
        MotionSensorProto dto = proto.getMotionSensor();

        return MotionSensorAvro.newBuilder().setLinkQuality(dto.getLinkQuality()).setMotion(dto.getMotion()).setVoltage(dto.getVoltage()).build();
    }

}
