"""Command-line entry point for the workflow engine."""

import argparse
import logging
import sys
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

import uvicorn
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings, load_settings
from workflow_engine.database import (
    DatabaseConfigurationError,
    check_database,
    database_engine,
)
from workflow_engine.logging import configure_logging
from workflow_engine.scheduler.entrypoint import run_scheduler
from workflow_engine.worker.entrypoint import run_worker


def _load_cli_settings(
    parser: argparse.ArgumentParser, env_file: Path | None
) -> Settings:
    """Keep startup errors consistent across commands without echoing input values."""
    try:
        return load_settings(env_file=env_file)
    except ValidationError as exc:
        errors = exc.errors(
            include_input=False, include_context=False, include_url=False
        )
        details = sorted(
            {
                f"{error['loc'][0] if error['loc'] else 'settings'} ({error['type']})"
                for error in errors
            }
        )
        parser.error("Invalid configuration: " + "; ".join(details))
    except (OSError, UnicodeError):
        parser.error("Environment file could not be read as UTF-8.")


def main() -> None:
    """Validate configuration before starting the selected process role."""
    parser = argparse.ArgumentParser(
        prog="engine",
        description=(
            "Distributed Workflow Engine. "
            "Publish workflows and run single-slot workers for ready tasks; "
            "Schedule DAG dependencies; crash recovery is not implemented yet."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('distributed-workflow-engine')}",
    )
    commands = parser.add_subparsers(dest="command")
    command_parsers: dict[str, argparse.ArgumentParser] = {}
    for name, help_text in (
        ("check-config", "Validate configuration without starting services."),
        ("api", "Start the HTTP API server."),
        ("worker", "Execute ready tasks from one Run with a single local slot."),
        ("scheduler", "Reconcile dependencies for one Run in short transactions."),
        ("check-db", "Check authenticated database connectivity; no schema changes."),
    ):
        command_parser = commands.add_parser(name, help=help_text)
        command_parser.add_argument(
            "--env-file",
            type=Path,
            help="Explicit UTF-8 dotenv file; environment variables take precedence.",
        )
        command_parsers[name] = command_parser

    command_parsers["worker"].add_argument("--run-id", type=UUID, required=True)
    command_parsers["worker"].add_argument(
        "--max-tasks",
        type=int,
        help="Exit after this many confirmed completions (including failures).",
    )
    command_parsers["scheduler"].add_argument("--run-id", type=UUID, required=True)
    command_parsers["scheduler"].add_argument("--once", action="store_true")

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        return

    settings = _load_cli_settings(command_parsers[args.command], args.env_file)
    if args.command == "scheduler":
        raise SystemExit(run_scheduler(settings, args.run_id, once=args.once))
    if args.command == "worker":
        if args.max_tasks is not None and args.max_tasks < 1:
            command_parsers["worker"].error("--max-tasks must be positive.")
        raise SystemExit(run_worker(settings, args.run_id, max_tasks=args.max_tasks))
    if args.command == "check-db":
        configure_logging(settings, component="cli")
        try:
            with database_engine(settings) as engine:
                check_database(engine)
        except DatabaseConfigurationError:
            print("Database credentials are missing or invalid.", file=sys.stderr)
            raise SystemExit(2) from None
        except SQLAlchemyError:
            logging.getLogger(__name__).error(
                "Database connectivity check failed.",
                extra={"event": "database_check_failed"},
            )
            raise SystemExit(1) from None
        print("Database connection is valid.")
        return

    if args.command == "check-config":
        configure_logging(settings, component="cli")
        logging.getLogger(__name__).info(
            "Configuration validation completed.",
            extra={"event": "configuration_validated"},
        )
        print("Configuration is valid.")
        return

    uvicorn.run(
        create_app(settings),
        host=str(settings.api_host),
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        access_log=False,
        proxy_headers=False,
        workers=1,
        lifespan="on",
        timeout_graceful_shutdown=10,
    )
