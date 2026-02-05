package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.Condition;

import java.util.List;

public interface ConditionRepository extends JpaRepository<Condition, Long> {
    void deleteAll(List<Condition> conditionsToDelete);
}
