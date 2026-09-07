"""Deterministic, iterative analysis of task dependency graphs."""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from heapq import heapify, heappop, heappush
from types import MappingProxyType

MAX_TASKS = 1000
MAX_EDGES = 10000


class DAGValidationError(ValueError):
    """A graph invariant violation with a stable code and relevant identifiers."""

    def __init__(self, code: str, message: str, task_ids: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.code = code
        self.task_ids = task_ids


@dataclass(frozen=True, slots=True)
class DAG:
    """An immutable analysis snapshot returned by analyze_dag."""

    topological_order: tuple[str, ...]
    roots: tuple[str, ...]
    dependencies: Mapping[str, tuple[str, ...]]
    dependents: Mapping[str, tuple[str, ...]]


def analyze_dag(dependencies: Mapping[str, Sequence[str]]) -> DAG:
    """Validate prerequisite edges and return indexes plus a topological order."""
    if not dependencies:
        raise DAGValidationError(
            "empty_dag", "A workflow must contain at least one task."
        )
    if len(dependencies) > MAX_TASKS:
        raise DAGValidationError(
            "too_many_tasks", f"A DAG supports at most {MAX_TASKS} tasks."
        )
    if sum(map(len, dependencies.values())) > MAX_EDGES:
        raise DAGValidationError(
            "too_many_edges", f"A DAG supports at most {MAX_EDGES} edges."
        )

    parents_by_id: dict[str, tuple[str, ...]] = {}
    children_by_id: dict[str, list[str]] = {task_id: [] for task_id in dependencies}
    for task_id in sorted(dependencies):
        parents = tuple(dependencies[task_id])
        repeated = tuple(
            sorted(parent for parent, count in Counter(parents).items() if count > 1)
        )
        if repeated:
            raise DAGValidationError(
                "duplicate_dependency",
                f"Task {task_id} repeats a dependency.",
                repeated,
            )
        if task_id in parents:
            raise DAGValidationError(
                "self_dependency", f"Task {task_id} depends on itself.", (task_id,)
            )
        missing = tuple(
            sorted(parent for parent in parents if parent not in dependencies)
        )
        if missing:
            raise DAGValidationError(
                "unknown_dependency",
                f"Task {task_id} references an unknown dependency.",
                missing,
            )
        parents_by_id[task_id] = tuple(sorted(parents))
        for parent in parents:
            children_by_id[parent].append(task_id)

    remaining = {task_id: len(parents) for task_id, parents in parents_by_id.items()}
    roots = tuple(task_id for task_id, count in remaining.items() if count == 0)
    frontier = list(roots)
    heapify(frontier)
    ordered: list[str] = []
    while frontier:
        task_id = heappop(frontier)
        ordered.append(task_id)
        for child in children_by_id[task_id]:
            remaining[child] -= 1
            if remaining[child] == 0:
                heappush(frontier, child)
    if len(ordered) != len(parents_by_id):
        blocked = tuple(task_id for task_id, count in remaining.items() if count > 0)
        raise DAGValidationError(
            "cycle_detected",
            "A dependency cycle prevents a complete topological order.",
            blocked,
        )

    return DAG(
        topological_order=tuple(ordered),
        roots=roots,
        dependencies=MappingProxyType(parents_by_id),
        dependents=MappingProxyType(
            {
                task_id: tuple(sorted(children_by_id[task_id]))
                for task_id in parents_by_id
            }
        ),
    )
