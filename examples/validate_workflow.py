"""Validate the bundled DAG example and print its static analysis."""

from pathlib import Path

from workflow_engine.domain.workflow import WorkflowDefinition


def main() -> None:
    source = Path(__file__).with_name("diamond.json")
    workflow = WorkflowDefinition.model_validate_json(
        source.read_text(encoding="utf-8")
    )
    dag = workflow.dag()
    print(f"Workflow: {workflow.name}")
    print(f"Roots: {', '.join(dag.roots)}")
    print(f"Topological order: {', '.join(dag.topological_order)}")
    print("Validation only; no tasks were executed.")


if __name__ == "__main__":
    main()
