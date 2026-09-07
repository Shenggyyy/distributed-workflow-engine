"""Command-line entry point for the workflow engine."""

import argparse
from importlib.metadata import version


def main() -> None:
    """Parse CLI arguments and display available commands."""
    parser = argparse.ArgumentParser(
        prog="engine",
        description=(
            "Distributed Workflow Engine. "
            "Project bootstrap only; workflow execution is not implemented yet."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('distributed-workflow-engine')}",
    )
    parser.parse_args()
    parser.print_help()
