"""Retain completion receipts bound to exact ownership and terminal outcome.

Revision ID: 0008
Revises: 0007
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Existing primary keys already guarantee uniqueness on populated databases.
    op.create_unique_constraint(
        "uq_task_attempts_id_status", "task_attempts", ["id", "status"]
    )
    op.create_unique_constraint(
        "uq_attempt_leases_owner_token",
        "attempt_leases",
        ["attempt_id", "worker_session_id", "lease_token"],
    )
    op.create_table(
        "attempt_completions",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("worker_session_id", sa.Uuid(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(16, collation="C"), nullable=False),
        sa.Column("error_code", sa.String(64, collation="C"), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("attempt_id", name=op.f("pk_attempt_completions")),
        sa.ForeignKeyConstraint(
            ["attempt_id", "worker_session_id", "lease_token"],
            [
                "attempt_leases.attempt_id",
                "attempt_leases.worker_session_id",
                "attempt_leases.lease_token",
            ],
            ondelete="RESTRICT",
            name="fk_attempt_completions_owner_lease",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id", "outcome"],
            ["task_attempts.id", "task_attempts.status"],
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
            name="fk_attempt_completions_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome IN ('SUCCEEDED', 'FAILED')",
            name=op.f("ck_attempt_completions_outcome_values"),
        ),
        sa.CheckConstraint(
            "(outcome = 'SUCCEEDED' AND error_code IS NULL) OR "
            "(outcome = 'FAILED' AND error_code IS NOT NULL "
            "AND error_code ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$')",
            name=op.f("ck_attempt_completions_error_code_format"),
        ),
        sa.CheckConstraint(
            "isfinite(accepted_at)", name=op.f("ck_attempt_completions_finite_time")
        ),
    )
    op.execute("""
        CREATE FUNCTION dwe_reject_completion_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='attempt completions are append-only';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER attempt_completions_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON attempt_completions
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_completion_mutation()
    """)


def downgrade() -> None:
    # Destructive to completion confirmations; keep all prior execution metadata.
    op.drop_table("attempt_completions")
    op.execute("DROP FUNCTION dwe_reject_completion_mutation()")
    op.drop_constraint(
        "uq_attempt_leases_owner_token", "attempt_leases", type_="unique"
    )
    op.drop_constraint("uq_task_attempts_id_status", "task_attempts", type_="unique")
