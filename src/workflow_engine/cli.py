"""Command-line entry point for the workflow engine."""

import argparse
import logging
from importlib.metadata import version
from pathlib import Path

from pydantic import ValidationError

from workflow_engine.config import load_settings
from workflow_engine.logging import configure_logging


def main() -> None:
    """Parse CLI arguments and run the requested command."""
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
    commands = parser.add_subparsers(dest="command")
    config_parser = commands.add_parser(
        "check-config",
        help="Validate application configuration without starting services.",
    )
    config_parser.add_argument(
        "--env-file",
        type=Path,
        help="Explicit UTF-8 dotenv file; environment variables take precedence.",
    )
    args = parser.parse_args()
    if args.command == "check-config":
        try:
            settings = load_settings(env_file=args.env_file)
        except ValidationError as exc:
            errors = exc.errors(
                include_input=False, include_context=False, include_url=False
            )
            details = sorted(
                {f"{error['loc'][0]} ({error['type']})" for error in errors}
            )
            config_parser.error("Invalid configuration: " + "; ".join(details))
        except (OSError, UnicodeError):
            config_parser.error("Environment file could not be read as UTF-8.")
        configure_logging(settings, component="cli")
        logging.getLogger(__name__).info(
            "Configuration validation completed.",
            extra={"event": "configuration_validated"},
        )
        print("Configuration is valid.")
        return
    parser.print_help()
