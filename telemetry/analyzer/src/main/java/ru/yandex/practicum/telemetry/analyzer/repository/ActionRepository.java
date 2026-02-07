package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.telemetry.analyzer.model.Action;

import java.util.List;

public interface ActionRepository extends JpaRepository<Action, Long> {

}