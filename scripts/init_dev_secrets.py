"""Create local development credentials without overwriting existing secrets."""

import argparse
import os
import secrets
from pathlib import Path


def initialize_secret(directory: Path) -> bool:
    """Return True if created; retain a nonempty existing secret unchanged."""
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "postgres_password.txt"
    try:
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if not destination.is_file() or not destination.read_bytes().strip():
            raise ValueError(
                "Existing PostgreSQL secret must be a nonempty file."
            ) from None
        return False

    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        stream.write(secrets.token_urlsafe(32) + "\n")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "secrets",
        help="Secret directory (default: repository secrets/).",
    )
    arguments = parser.parse_args()
    try:
        created = initialize_secret(arguments.directory)
    except (OSError, ValueError):
        parser.exit(
            1,
            "Unable to initialize secret; check directory access and existing file.\n",
        )
    print("Development secret created." if created else "Existing secret retained.")


if __name__ == "__main__":
    main()
