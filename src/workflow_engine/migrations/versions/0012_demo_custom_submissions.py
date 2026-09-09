"""Add immutable custom demonstration receipts without rewriting existing Runs."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(op.f("ck_demo_runs_scenario_values"), "demo_runs", type_="check")
    op.create_check_constraint(
        op.f("ck_demo_runs_scenario_values"),
        "demo_runs",
        "scenario IN ('parallel', 'distribution', 'recovery', 'custom')",
    )
    op.create_table(
        "demo_custom_submissions",
        sa.Column("idempotency_key", sa.String(128, collation="C"), nullable=False),
        sa.Column("definition", JSONB(none_as_null=True), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "idempotency_key", name=op.f("pk_demo_custom_submissions")
        ),
        sa.UniqueConstraint("run_id", name=op.f("uq_demo_custom_submissions_run_id")),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["demo_runs.run_id"],
            name=op.f("fk_demo_custom_submissions_run_id_demo_runs"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'",
            name=op.f("ck_demo_custom_submissions_key_format"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(definition) = 'object'",
            name=op.f("ck_demo_custom_submissions_definition_object"),
        ),
        sa.CheckConstraint(
            "isfinite(created_at)",
            name=op.f("ck_demo_custom_submissions_finite_time"),
        ),
    )
    op.execute("""
        CREATE TRIGGER demo_custom_submissions_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON demo_custom_submissions
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_demo_mutation()
    """)


def downgrade() -> None:
    # Custom submission reads receipts before inserting membership. Take table
    # locks in that same order; hold them through the check and constraint change
    # so a competing insertion cannot slip past the check or invert lock order.
    op.execute("LOCK TABLE demo_custom_submissions IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE demo_runs IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM demo_runs WHERE scenario = 'custom')
                OR EXISTS (SELECT 1 FROM demo_custom_submissions) THEN
                RAISE EXCEPTION USING ERRCODE='55000',
                    MESSAGE='custom demo history prevents downgrade to 0011';
            END IF;
        END; $$
    """)
    op.drop_table("demo_custom_submissions")
    op.drop_constraint(op.f("ck_demo_runs_scenario_values"), "demo_runs", type_="check")
    op.create_check_constraint(
        op.f("ck_demo_runs_scenario_values"),
        "demo_runs",
        "scenario IN ('parallel', 'distribution', 'recovery')",
    )
