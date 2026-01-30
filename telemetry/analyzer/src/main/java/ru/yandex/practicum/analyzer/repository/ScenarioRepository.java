package ru.yandex.practicum.analyzer.repository;

import ru.yandex.practicum.analyzer.model.Scenario;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    List<Scenario> findByHubId(String hubId);
    Optional<Scenario> findByHubIdAndName(String hubId, String name);
}