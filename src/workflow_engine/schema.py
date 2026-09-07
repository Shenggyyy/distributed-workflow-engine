"""SQLAlchemy table definitions and shared constraint naming conventions."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    Uuid,
    func,
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
