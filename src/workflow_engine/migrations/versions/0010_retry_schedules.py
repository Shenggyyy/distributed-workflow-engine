"""Persist append-only retry eligibility for terminal failed Attempts.

Revision ID: 0010
Revises: 0009
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "task_retry_schedules",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(16, collation="C"), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("attempt_id", name=op.f("pk_task_retry_schedules")),
        sa.ForeignKeyConstraint(
            ["attempt_id", "outcome"],
            ["task_attempts.id", "task_attempts.status"],
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
            name="fk_task_retry_schedules_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome IN ('FAILED', 'TIMED_OUT', 'LOST')",
            name=op.f("ck_task_retry_schedules_outcome_values"),
        ),
        sa.CheckConstraint(
            "isfinite(scheduled_at) AND isfinite(available_at) "
            "AND scheduled_at < available_at",
            name=op.f("ck_task_retry_schedules_time_order"),
        ),
    )
    op.execute("""
        CREATE FUNCTION dwe_reject_retry_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='retry schedules are append-only';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER task_retry_schedules_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON task_retry_schedules
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_retry_mutation()
    """)


def downgrade() -> None:
    # Destructive to retry eligibility; stop writers and explicitly plan rollback.
    op.drop_table("task_retry_schedules")
    op.execute("DROP FUNCTION dwe_reject_retry_mutation()")
