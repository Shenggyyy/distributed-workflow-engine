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
