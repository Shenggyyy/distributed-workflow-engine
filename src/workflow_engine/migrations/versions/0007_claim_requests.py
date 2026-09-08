"""Retain immutable completed claim decisions without inventing request identities.

Revision ID: 0007
Revises: 0006
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # The existing attempt PK makes this unique for populated databases too.
    # PostgreSQL needs this composite key to enforce the binding's exact owner.
    op.create_unique_constraint(
        op.f("uq_attempt_leases_attempt_id"),
        "attempt_leases",
        ["attempt_id", "worker_session_id"],
    )
    op.create_table(
        "claim_requests",
        sa.Column("worker_session_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "worker_session_id", "request_id", name=op.f("pk_claim_requests")
        ),
        sa.UniqueConstraint("attempt_id", name=op.f("uq_claim_requests_attempt_id")),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            ondelete="RESTRICT",
            name=op.f("fk_claim_requests_worker_session_id_worker_sessions"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            ondelete="RESTRICT",
            name=op.f("fk_claim_requests_run_id_workflow_runs"),
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id", "worker_session_id"],
            ["attempt_leases.attempt_id", "attempt_leases.worker_session_id"],
            ondelete="RESTRICT",
            name=op.f("fk_claim_requests_attempt_id_attempt_leases"),
        ),
        sa.CheckConstraint(
            "isfinite(created_at)", name=op.f("ck_claim_requests_finite_time")
        ),
    )
    op.create_index("ix_claim_requests_run_id", "claim_requests", ["run_id"])
    op.execute("""
        CREATE FUNCTION dwe_reject_claim_request_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='claim requests are append-only';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER claim_requests_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON claim_requests
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_claim_request_mutation()
    """)


def downgrade() -> None:
    # Destructive for deduplication history; preserves all attempts and leases.
    op.drop_table("claim_requests")
    op.execute("DROP FUNCTION dwe_reject_claim_request_mutation()")
    op.drop_constraint(
        op.f("uq_attempt_leases_attempt_id"), "attempt_leases", type_="unique"
    )
