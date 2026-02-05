package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;

import java.util.List;

public interface ConditionRepository extends JpaRepository<Condition, Long> {
    void deleteAll(List<Condition> conditionsToDelete);
}
