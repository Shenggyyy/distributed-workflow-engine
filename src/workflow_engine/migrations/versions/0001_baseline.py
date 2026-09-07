"""Establish revision history without creating business tables."""

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Alembic itself creates and records the version table."""


def downgrade() -> None:
    """There are no business schema changes to reverse."""
