package ru.yandex.practicum.telemetry.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.telemetry.aggregator.service.AggregationStarter;

@Slf4j
@SpringBootApplication
public class Aggregator {

    public static void main(String[] args) {
        log.info("Запуск сервиса Aggregator...");
        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);
        AggregationStarter aggregator = context.getBean(AggregationStarter.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Получен сигнал завершения работы");
            aggregator.shutdown();
        }));

        log.info("Запуск основного цикла обработки событий");
        aggregator.start();

    }
}
