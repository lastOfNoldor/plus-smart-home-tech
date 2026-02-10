package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.sensor_event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.sensor_event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;


@Component
public class ClimateSensorEventHandler extends BaseSensorEventHandler<ClimateSensorAvro> {

    public ClimateSensorEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.CLIMATE_SENSOR;
    }

    @Override
    protected ClimateSensorAvro mapToAvro(SensorEventProto proto) {
        ClimateSensorProto dto = proto.getClimateSensor();

        return ClimateSensorAvro.newBuilder().setTemperatureC(dto.getTemperatureC()).setHumidity(dto.getHumidity()).setCo2Level(dto.getCo2Level()).build();
    }



}
