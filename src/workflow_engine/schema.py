"""SQLAlchemy table definitions and shared constraint naming conventions."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

workflows = Table(
    "workflows",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("name", String(64, collation="C"), nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    UniqueConstraint("name"),
    CheckConstraint("name ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'", name="name_format"),
)

workflow_versions = Table(
    "workflow_versions",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column(
        "workflow_id",
        Uuid,
        ForeignKey("workflows.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("version_number", Integer, nullable=False),
    Column("definition", JSONB(none_as_null=True), nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    UniqueConstraint("workflow_id", "version_number"),
    CheckConstraint("version_number > 0", name="version_number_positive"),
    CheckConstraint("jsonb_typeof(definition) = 'object'", name="definition_object"),
)


workflow_runs = Table(
    "workflow_runs",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column(
        "workflow_version_id",
        Uuid,
        ForeignKey("workflow_versions.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "status",
        String(16, collation="C"),
        nullable=False,
        server_default=text("'PENDING'"),
    ),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint(
        "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')", name="status_values"
    ),
    UniqueConstraint("id", "workflow_version_id"),
)

task_runs = Table(
    "task_runs",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column(
        "run_id",
        Uuid,
        ForeignKey("workflow_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("task_key", String(64, collation="C"), nullable=False),
    Column(
        "status",
        String(16, collation="C"),
        nullable=False,
        server_default=text("'PENDING'"),
    ),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint(
        "status IN ('PENDING', 'READY', 'RUNNING', 'RETRY_WAIT', "
        "'SUCCEEDED', 'FAILED', 'SKIPPED')",
        name="status_values",
    ),
    UniqueConstraint("run_id", "task_key"),
    CheckConstraint(
        "task_key ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'", name="task_key_format"
    ),
)

task_attempts = Table(
    "task_attempts",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column(
        "task_id", Uuid, ForeignKey("task_runs.id", ondelete="RESTRICT"), nullable=False
    ),
    Column("attempt_number", Integer, nullable=False),
    Column(
        "status",
        String(16, collation="C"),
        nullable=False,
        server_default=text("'RUNNING'"),
    ),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint(
        "status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST')",
        name="status_values",
    ),
    UniqueConstraint("task_id", "attempt_number"),
    UniqueConstraint("id", "status", name="uq_task_attempts_id_status"),
    CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
)

Index("ix_workflow_runs_workflow_version_id", workflow_runs.c.workflow_version_id)
Index(
    "uq_task_attempts_one_running_per_task",
    task_attempts.c.task_id,
    unique=True,
    postgresql_where=text("status = 'RUNNING'"),
)

# Existing unkeyed runs remain valid. The optional request row is immutable and
# its composite reference is checked when the creating transaction commits.
run_creation_requests = Table(
    "run_creation_requests",
    metadata,
    Column("idempotency_key", String(128, collation="C"), primary_key=True),
    Column("workflow_version_id", Uuid, nullable=False),
    Column("run_id", Uuid, nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    UniqueConstraint("run_id"),
    CheckConstraint(
        "idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'", name="key_format"
    ),
    ForeignKeyConstraint(
        ["run_id", "workflow_version_id"],
        ["workflow_runs.id", "workflow_runs.workflow_version_id"],
        ondelete="NO ACTION",
        deferrable=True,
        initially="DEFERRED",
    ),
)


# Session times are explicit inputs from the future registration/heartbeat
# transaction. No independent defaults may silently use a stale transaction time.
worker_sessions = Table(
    "worker_sessions",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("worker_name", String(64, collation="C"), nullable=False),
    Column("max_concurrency", Integer, nullable=False),
    Column(
        "status",
        String(16, collation="C"),
        nullable=False,
        server_default=text("'ACTIVE'"),
    ),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("last_heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("heartbeat_expires_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "worker_name ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$'", name="name_format"
    ),
    CheckConstraint("max_concurrency > 0", name="concurrency_positive"),
    CheckConstraint("status IN ('ACTIVE', 'LOST', 'STOPPED')", name="status_values"),
    CheckConstraint(
        "isfinite(created_at) AND isfinite(last_heartbeat_at) "
        "AND isfinite(heartbeat_expires_at)",
        name="finite_times",
    ),
    CheckConstraint(
        "created_at <= last_heartbeat_at AND last_heartbeat_at < heartbeat_expires_at",
        name="heartbeat_order",
    ),
)
Index(
    "ix_worker_sessions_active_deadline",
    worker_sessions.c.heartbeat_expires_at,
    worker_sessions.c.id,
    postgresql_where=text("status = 'ACTIVE'"),
)


# Optional for historical Attempts; future claim transactions create both rows.
# Current attempt status and clock checks belong in the ordered transaction.
attempt_leases = Table(
    "attempt_leases",
    metadata,
    Column(
        "attempt_id",
        Uuid,
        ForeignKey("task_attempts.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column(
        "worker_session_id",
        Uuid,
        ForeignKey("worker_sessions.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("lease_token", Uuid, nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
    Column("last_renewed_at", DateTime(timezone=True), nullable=False),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("lease_token"),
    UniqueConstraint("attempt_id", "worker_session_id"),
    UniqueConstraint(
        "attempt_id",
        "worker_session_id",
        "lease_token",
        name="uq_attempt_leases_owner_token",
    ),
    CheckConstraint(
        "isfinite(acquired_at) AND isfinite(last_renewed_at) "
        "AND isfinite(lease_expires_at)",
        name="finite_times",
    ),
    CheckConstraint(
        "acquired_at <= last_renewed_at AND last_renewed_at < lease_expires_at",
        name="lease_order",
    ),
)
Index(
    "ix_attempt_leases_worker_attempt",
    attempt_leases.c.worker_session_id,
    attempt_leases.c.attempt_id,
)
Index(
    "ix_attempt_leases_deadline",
    attempt_leases.c.lease_expires_at,
    attempt_leases.c.attempt_id,
)


# A completed allocation decision, never an in-progress reservation. NULL attempt
# means a durable no-work result; only the future ordered repository authorizes it.
claim_requests = Table(
    "claim_requests",
    metadata,
    Column(
        "worker_session_id",
        Uuid,
        ForeignKey("worker_sessions.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("request_id", Uuid, primary_key=True),
    Column(
        "run_id",
        Uuid,
        ForeignKey("workflow_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("attempt_id", Uuid, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("attempt_id"),
    ForeignKeyConstraint(
        ["attempt_id", "worker_session_id"],
        ["attempt_leases.attempt_id", "attempt_leases.worker_session_id"],
        ondelete="RESTRICT",
    ),
    CheckConstraint("isfinite(created_at)", name="finite_time"),
)
Index("ix_claim_requests_run_id", claim_requests.c.run_id)


# Optional immutable Worker receipt. Engine-generated LOST/TIMED_OUT and legacy
# terminal Attempts remain valid without one. Time authorization is transactional.
attempt_completions = Table(
    "attempt_completions",
    metadata,
    Column("attempt_id", Uuid, primary_key=True),
    Column("worker_session_id", Uuid, nullable=False),
    Column("lease_token", Uuid, nullable=False),
    Column("outcome", String(16, collation="C"), nullable=False),
    Column("error_code", String(64, collation="C"), nullable=True),
    Column("accepted_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(
        ["attempt_id", "worker_session_id", "lease_token"],
        [
            "attempt_leases.attempt_id",
            "attempt_leases.worker_session_id",
            "attempt_leases.lease_token",
        ],
        ondelete="RESTRICT",
        name="fk_attempt_completions_owner_lease",
    ),
    ForeignKeyConstraint(
        ["attempt_id", "outcome"],
        ["task_attempts.id", "task_attempts.status"],
        ondelete="NO ACTION",
        deferrable=True,
        initially="DEFERRED",
        name="fk_attempt_completions_attempt_outcome",
    ),
    CheckConstraint("outcome IN ('SUCCEEDED', 'FAILED')", name="outcome_values"),
    CheckConstraint(
        "(outcome = 'SUCCEEDED' AND error_code IS NULL) OR "
        "(outcome = 'FAILED' AND error_code IS NOT NULL "
        "AND error_code ~ '^[A-Za-z][A-Za-z0-9_-]{0,63}$')",
        name="error_code_format",
    ),
    CheckConstraint("isfinite(accepted_at)", name="finite_time"),
)
