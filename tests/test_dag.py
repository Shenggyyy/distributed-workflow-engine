"""DAG invariants, deterministic ordering, and bounded iterative traversal."""

from itertools import permutations
from random import Random

import pytest

from workflow_engine.domain.dag import MAX_EDGES, DAGValidationError, analyze_dag


def test_parallel_roots_and_disconnected_components_are_allowed() -> None:
    dag = analyze_dag({"D": ["B", "C"], "C": ["A"], "B": ["A"], "A": [], "E": []})
    assert dag.roots == ("A", "E")
    assert dag.topological_order == ("A", "B", "C", "D", "E")
    assert dag.dependents["A"] == ("B", "C")


def test_order_is_independent_of_task_and_dependency_declaration_order() -> None:
    graph = {"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]}
    expected = analyze_dag(graph)
    for order in permutations(graph):
        reordered = {task: list(reversed(graph[task])) for task in order}
        assert analyze_dag(reordered) == expected


def test_graph_snapshot_cannot_be_mutated_through_input_or_indexes() -> None:
    parents = ["A"]
    dag = analyze_dag({"A": [], "B": parents})
    parents.clear()
    assert dag.dependencies["B"] == ("A",)
    with pytest.raises(TypeError):
        dag.dependencies["B"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        dag.dependents["A"] = ()  # type: ignore[index]


def test_cycle_report_includes_blocked_descendants_but_not_unrelated_roots() -> None:
    with pytest.raises(DAGValidationError) as error:
        analyze_dag({"A": ["B"], "B": ["A"], "C": ["B"], "D": []})
    assert error.value.code == "cycle_detected"
    assert error.value.task_ids == ("A", "B", "C")


def test_long_chain_does_not_need_recursion() -> None:
    ids = [f"t{i:04}" for i in range(1000)]
    graph = {task: [ids[i - 1]] if i else [] for i, task in enumerate(ids)}
    assert analyze_dag(graph).topological_order == tuple(ids)


def test_edge_limit_is_inclusive() -> None:
    parents = [f"p{i:03}" for i in range(100)]
    graph: dict[str, list[str]] = {parent: [] for parent in parents}
    graph.update({f"c{i:03}": parents.copy() for i in range(100)})
    dag = analyze_dag(graph)
    assert sum(map(len, dag.dependencies.values())) == MAX_EDGES
    graph["extra"] = [parents[0]]
    with pytest.raises(DAGValidationError) as error:
        analyze_dag(graph)
    assert error.value.code == "too_many_edges"


@pytest.mark.parametrize("graph", [{}, {f"t{i}": [] for i in range(1001)}])
def test_graph_size_limits(graph: dict[str, list[str]]) -> None:
    with pytest.raises(DAGValidationError):
        analyze_dag(graph)


def test_generated_dags_preserve_every_edge_and_visit_each_task_once() -> None:
    random = Random(42)
    for _ in range(30):
        ids = [f"t{i:02}" for i in range(40)]
        random.shuffle(ids)
        graph = {
            task: [parent for parent in ids[:i] if random.random() < 0.12]
            for i, task in enumerate(ids)
        }
        dag = analyze_dag(graph)
        positions = {task: i for i, task in enumerate(dag.topological_order)}
        assert len(positions) == len(ids)
        assert set(positions) == set(ids)
        for child, parents in graph.items():
            assert set(dag.dependencies[child]) == set(parents)
            for parent in parents:
                assert positions[parent] < positions[child]
                assert child in dag.dependents[parent]
