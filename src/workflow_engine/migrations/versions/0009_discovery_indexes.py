"""Add partial indexes for bounded active Run discovery.

Revision ID: 0009
Revises: 0008
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "ix_workflow_runs_active_id",
        "workflow_runs",
        ["id"],
        postgresql_where=sa.text("status = 'RUNNING'"),
    )
    op.create_index(
        "ix_task_runs_ready_run_id",
        "task_runs",
        ["run_id"],
        postgresql_where=sa.text("status = 'READY'"),
    )


def downgrade() -> None:
    op.drop_index("ix_task_runs_ready_run_id", table_name="task_runs")
    op.drop_index("ix_workflow_runs_active_id", table_name="workflow_runs")
