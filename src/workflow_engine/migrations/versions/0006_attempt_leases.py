"""Store attempt lease identities and monotonic metadata without fabricating owners.

Revision ID: 0006
Revises: 0005
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "attempt_leases",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("worker_session_id", sa.Uuid(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_renewed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("attempt_id", name=op.f("pk_attempt_leases")),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["task_attempts.id"],
            ondelete="RESTRICT",
            name=op.f("fk_attempt_leases_attempt_id_task_attempts"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            ondelete="RESTRICT",
            name=op.f("fk_attempt_leases_worker_session_id_worker_sessions"),
        ),
        sa.UniqueConstraint("lease_token", name=op.f("uq_attempt_leases_lease_token")),
        sa.CheckConstraint(
            "isfinite(acquired_at) AND isfinite(last_renewed_at) "
            "AND isfinite(lease_expires_at)",
            name=op.f("ck_attempt_leases_finite_times"),
        ),
        sa.CheckConstraint(
            "acquired_at <= last_renewed_at AND last_renewed_at < lease_expires_at",
            name=op.f("ck_attempt_leases_lease_order"),
        ),
    )
    op.create_index(
        "ix_attempt_leases_worker_attempt",
        "attempt_leases",
        ["worker_session_id", "attempt_id"],
    )
    op.create_index(
        "ix_attempt_leases_deadline",
        "attempt_leases",
        ["lease_expires_at", "attempt_id"],
    )
    op.execute("""
        CREATE FUNCTION dwe_guard_attempt_lease_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF ROW(NEW.attempt_id, NEW.worker_session_id,
                   NEW.lease_token, NEW.acquired_at)
                IS DISTINCT FROM
                ROW(OLD.attempt_id, OLD.worker_session_id,
                    OLD.lease_token, OLD.acquired_at)
            THEN
                RAISE EXCEPTION USING ERRCODE='55000',
                    MESSAGE='attempt lease identity is immutable';
            END IF;
            IF NEW.last_renewed_at < OLD.last_renewed_at
                OR NEW.lease_expires_at < OLD.lease_expires_at
            THEN
                RAISE EXCEPTION USING ERRCODE='55000',
                    MESSAGE='attempt lease time cannot move backwards';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER attempt_leases_guard_update BEFORE UPDATE ON attempt_leases
        FOR EACH ROW EXECUTE FUNCTION dwe_guard_attempt_lease_update()
    """)
    op.execute("""
        CREATE FUNCTION dwe_reject_attempt_lease_deletion() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='attempt lease history cannot be deleted';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER attempt_leases_retain_history
        BEFORE DELETE OR TRUNCATE ON attempt_leases
        FOR EACH STATEMENT EXECUTE FUNCTION dwe_reject_attempt_lease_deletion()
    """)


def downgrade() -> None:
    # Destructive for ownership history, while preserving all older runtime rows.
    op.drop_table("attempt_leases")
    op.execute("DROP FUNCTION dwe_reject_attempt_lease_deletion()")
    op.execute("DROP FUNCTION dwe_guard_attempt_lease_update()")
