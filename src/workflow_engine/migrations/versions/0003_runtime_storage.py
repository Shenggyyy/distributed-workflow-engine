"""Add runtime identity/status storage and structural lifecycle guards.

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("workflow_version_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.String(16, collation="C"),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_runs")),
        sa.ForeignKeyConstraint(
            ["workflow_version_id"],
            ["workflow_versions.id"],
            name=op.f("fk_workflow_runs_workflow_version_id_workflow_versions"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')",
            name=op.f("ck_workflow_runs_status_values"),
        ),
    )
    op.create_table(
        "task_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("task_key", sa.String(64, collation="C"), nullable=False),
        sa.Column(
            "status",
            sa.String(16, collation="C"),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_runs")),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_task_runs_run_id_workflow_runs"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'READY', 'RUNNING', 'RETRY_WAIT', "
            "'SUCCEEDED', 'FAILED', 'SKIPPED')",
            name=op.f("ck_task_runs_status_values"),
        ),
        sa.UniqueConstraint("run_id", "task_key", name=op.f("uq_task_runs_run_id")),
        sa.CheckConstraint(
            "task_key ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'",
            name=op.f("ck_task_runs_task_key_format"),
        ),
    )
    op.create_table(
        "task_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(16, collation="C"),
            nullable=False,
            server_default=sa.text("'RUNNING'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_attempts")),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["task_runs.id"],
            name=op.f("fk_task_attempts_task_id_task_runs"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST')",
            name=op.f("ck_task_attempts_status_values"),
        ),
        sa.UniqueConstraint(
            "task_id", "attempt_number", name=op.f("uq_task_attempts_task_id")
        ),
        sa.CheckConstraint(
            "attempt_number > 0", name=op.f("ck_task_attempts_attempt_number_positive")
        ),
    )
    op.create_index(
        "ix_workflow_runs_workflow_version_id", "workflow_runs", ["workflow_version_id"]
    )
    op.create_index(
        "uq_task_attempts_one_running_per_task",
        "task_attempts",
        ["task_id"],
        unique=True,
        postgresql_where=sa.text("status = 'RUNNING'"),
    )
    op.execute("""
        CREATE FUNCTION dwe_guard_runtime_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            identity_changed boolean;
            allowed_transition boolean;
        BEGIN
            IF TG_TABLE_NAME = 'workflow_runs' THEN
                identity_changed := ROW(
                        NEW.id,
                        NEW.workflow_version_id,
                        NEW.created_at
                    )
                    IS DISTINCT FROM ROW(
                        OLD.id,
                        OLD.workflow_version_id,
                        OLD.created_at
                    );
                allowed_transition :=
                    (OLD.status = 'PENDING' AND NEW.status = 'RUNNING')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'SUCCEEDED')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'FAILED');
            ELSIF TG_TABLE_NAME = 'task_runs' THEN
                identity_changed := ROW(
                        NEW.id,
                        NEW.run_id,
                        NEW.task_key,
                        NEW.created_at
                    )
                    IS DISTINCT FROM ROW(
                        OLD.id,
                        OLD.run_id,
                        OLD.task_key,
                        OLD.created_at
                    );
                allowed_transition :=
                    (OLD.status = 'PENDING' AND NEW.status = 'READY')
                    OR (OLD.status = 'PENDING' AND NEW.status = 'SKIPPED')
                    OR (OLD.status = 'READY' AND NEW.status = 'RUNNING')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'SUCCEEDED')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'RETRY_WAIT')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'FAILED')
                    OR (OLD.status = 'RETRY_WAIT' AND NEW.status = 'READY');
            ELSIF TG_TABLE_NAME = 'task_attempts' THEN
                identity_changed := ROW(
                        NEW.id,
                        NEW.task_id,
                        NEW.attempt_number,
                        NEW.created_at
                    )
                    IS DISTINCT FROM ROW(
                        OLD.id,
                        OLD.task_id,
                        OLD.attempt_number,
                        OLD.created_at
                    );
                allowed_transition :=
                    (OLD.status = 'RUNNING' AND NEW.status = 'SUCCEEDED')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'FAILED')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'TIMED_OUT')
                    OR (OLD.status = 'RUNNING' AND NEW.status = 'LOST');
            ELSE
                RAISE EXCEPTION USING
                    ERRCODE='55000',
                    MESSAGE='unexpected runtime table';
            END IF;
            IF identity_changed THEN
                RAISE EXCEPTION USING
                    ERRCODE='55000',
                    MESSAGE='runtime identity is immutable';
            END IF;
            -- An unchanged status is a SQL no-op, not a replayed domain event.
            IF NEW.status IS DISTINCT FROM OLD.status
                AND allowed_transition IS NOT TRUE THEN
                RAISE EXCEPTION USING
                    ERRCODE='55000',
                    MESSAGE='illegal runtime state transition';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE FUNCTION dwe_reject_runtime_deletion() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING
                    ERRCODE='55000',
                    MESSAGE='runtime history cannot be deleted';
        END;
        $$
    """)
    for table in ("workflow_runs", "task_runs", "task_attempts"):
        # These are fixed identifiers belonging to this frozen revision.
        op.execute(f"""
            CREATE TRIGGER {table}_guard_update
            BEFORE UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION dwe_guard_runtime_update()
        """)
        op.execute(f"""
            CREATE TRIGGER {table}_retain_history
            BEFORE DELETE OR TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_runtime_deletion()
        """)


def downgrade() -> None:
    # Destructive, but intentionally leaves 0002's published definitions intact.
    op.drop_table("task_attempts")
    op.drop_table("task_runs")
    op.drop_table("workflow_runs")
    op.execute("DROP FUNCTION dwe_reject_runtime_deletion()")
    op.execute("DROP FUNCTION dwe_guard_runtime_update()")
