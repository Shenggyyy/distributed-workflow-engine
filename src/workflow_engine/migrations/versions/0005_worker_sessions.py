"""Persist worker sessions and structural lifecycle/heartbeat guards.

Revision ID: 0005
Revises: 0004
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "worker_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("worker_name", sa.String(64, collation="C"), nullable=False),
        sa.Column("max_concurrency", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(16, collation="C"),
            nullable=False,
            server_default=sa.text("'ACTIVE'"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_worker_sessions")),
        sa.CheckConstraint(
            "worker_name ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'",
            name=op.f("ck_worker_sessions_name_format"),
        ),
        sa.CheckConstraint(
            "max_concurrency > 0",
            name=op.f("ck_worker_sessions_concurrency_positive"),
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'LOST', 'STOPPED')",
            name=op.f("ck_worker_sessions_status_values"),
        ),
        sa.CheckConstraint(
            "isfinite(created_at) AND isfinite(last_heartbeat_at) "
            "AND isfinite(heartbeat_expires_at)",
            name=op.f("ck_worker_sessions_finite_times"),
        ),
        sa.CheckConstraint(
            "created_at <= last_heartbeat_at "
            "AND last_heartbeat_at < heartbeat_expires_at",
            name=op.f("ck_worker_sessions_heartbeat_order"),
        ),
    )
    op.create_index(
        "ix_worker_sessions_active_deadline",
        "worker_sessions",
        ["heartbeat_expires_at", "id"],
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )
    op.execute("""
        CREATE FUNCTION dwe_guard_worker_session_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF ROW(NEW.id, NEW.worker_name, NEW.max_concurrency, NEW.created_at)
                IS DISTINCT FROM
                ROW(OLD.id, OLD.worker_name, OLD.max_concurrency, OLD.created_at)
            THEN
                RAISE EXCEPTION USING ERRCODE='55000',
                    MESSAGE='worker session identity is immutable';
            END IF;
            IF NEW.status IS DISTINCT FROM OLD.status AND
                (OLD.status = 'ACTIVE' AND NEW.status IN ('LOST', 'STOPPED'))
                    IS NOT TRUE
            THEN
                RAISE EXCEPTION USING ERRCODE='55000',
                    MESSAGE='illegal worker session state transition';
            END IF;
            IF ROW(NEW.last_heartbeat_at, NEW.heartbeat_expires_at)
                IS DISTINCT FROM ROW(OLD.last_heartbeat_at, OLD.heartbeat_expires_at)
            THEN
                IF OLD.status <> 'ACTIVE' OR NEW.status <> 'ACTIVE' THEN
                    RAISE EXCEPTION USING ERRCODE='55000',
                        MESSAGE='terminal worker heartbeat is immutable';
                END IF;
                IF NEW.last_heartbeat_at < OLD.last_heartbeat_at
                    OR NEW.heartbeat_expires_at < OLD.heartbeat_expires_at
                THEN
                    RAISE EXCEPTION USING ERRCODE='55000',
                        MESSAGE='worker heartbeat cannot move backwards';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER worker_sessions_guard_update
        BEFORE UPDATE ON worker_sessions
        FOR EACH ROW EXECUTE FUNCTION dwe_guard_worker_session_update()
    """)
    op.execute("""
        CREATE FUNCTION dwe_reject_worker_session_deletion() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='worker session history cannot be deleted';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER worker_sessions_retain_history
        BEFORE DELETE OR TRUNCATE ON worker_sessions
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_worker_session_deletion()
    """)


def downgrade() -> None:
    # Loses session identity/liveness history; existing runs and bindings survive.
    op.drop_table("worker_sessions")
    op.execute("DROP FUNCTION dwe_reject_worker_session_deletion()")
    op.execute("DROP FUNCTION dwe_guard_worker_session_update()")
