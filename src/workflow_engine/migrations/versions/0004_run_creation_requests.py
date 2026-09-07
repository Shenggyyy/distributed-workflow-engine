"""Add durable, immutable run-creation request bindings.

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # The composite foreign key must bind the requested version to this exact run.
    op.create_unique_constraint(
        op.f("uq_workflow_runs_id"), "workflow_runs", ["id", "workflow_version_id"]
    )
    op.create_table(
        "run_creation_requests",
        sa.Column("idempotency_key", sa.String(128, collation="C"), nullable=False),
        sa.Column("workflow_version_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "idempotency_key", name=op.f("pk_run_creation_requests")
        ),
        sa.UniqueConstraint("run_id", name=op.f("uq_run_creation_requests_run_id")),
        sa.CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'",
            name=op.f("ck_run_creation_requests_key_format"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "workflow_version_id"],
            ["workflow_runs.id", "workflow_runs.workflow_version_id"],
            name=op.f("fk_run_creation_requests_run_id_workflow_runs"),
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    op.execute("""
        CREATE FUNCTION dwe_reject_run_request_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING
                ERRCODE='55000',
                MESSAGE='run creation requests are append-only';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER run_creation_requests_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON run_creation_requests
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_run_request_mutation()
    """)


def downgrade() -> None:
    # Removes deduplication history, not runs. Replaying old keys is then unsafe.
    op.drop_table("run_creation_requests")
    op.execute("DROP FUNCTION dwe_reject_run_request_mutation()")
    op.drop_constraint(op.f("uq_workflow_runs_id"), "workflow_runs", type_="unique")
