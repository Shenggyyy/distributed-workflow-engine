"""Create workflows and append-only definition versions.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(64, collation="C"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "name ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'",
            name=op.f("ck_workflows_name_format"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflows")),
        sa.UniqueConstraint("name", name=op.f("uq_workflows_name")),
    )
    op.create_table(
        "workflow_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("definition", postgresql.JSONB(none_as_null=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "version_number > 0",
            name=op.f("ck_workflow_versions_version_number_positive"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(definition) = 'object'",
            name=op.f("ck_workflow_versions_definition_object"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflows.id"],
            name=op.f("fk_workflow_versions_workflow_id_workflows"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_versions")),
        sa.UniqueConstraint(
            "workflow_id",
            "version_number",
            name=op.f("uq_workflow_versions_workflow_id"),
        ),
    )
    op.execute("""
        CREATE FUNCTION dwe_reject_workflow_version_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'workflow_versions are append-only';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER workflow_versions_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON workflow_versions
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_workflow_version_mutation()
    """)


def downgrade() -> None:
    # Destructive: dropping the table also drops its mutation-guard trigger.
    op.drop_table("workflow_versions")
    op.execute("DROP FUNCTION dwe_reject_workflow_version_mutation()")
    op.drop_table("workflows")
