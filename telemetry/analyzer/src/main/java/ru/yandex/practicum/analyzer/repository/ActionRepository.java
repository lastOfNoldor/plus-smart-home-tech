package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.Action;

import java.util.List;

public interface ActionRepository extends JpaRepository<Action, Long> {
    void deleteAll(List<Action> actionsToDelete);
}