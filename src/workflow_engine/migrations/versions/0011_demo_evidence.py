"""Add optional demonstration membership and append-only Handler evidence."""

from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE demo_runs (
            run_id UUID CONSTRAINT pk_demo_runs PRIMARY KEY,
            scenario VARCHAR(16) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT fk_demo_runs_run_id_workflow_runs FOREIGN KEY (run_id)
                REFERENCES workflow_runs(id) ON DELETE RESTRICT,
            CONSTRAINT ck_demo_runs_scenario_values CHECK
                (scenario IN ('parallel', 'distribution', 'recovery'))
        )
    """)
    op.execute("""
        CREATE TABLE demo_workers (
            worker_session_id UUID CONSTRAINT pk_demo_workers PRIMARY KEY,
            run_id UUID NOT NULL,
            CONSTRAINT fk_demo_workers_worker_session_id_worker_sessions
                FOREIGN KEY (worker_session_id) REFERENCES worker_sessions(id)
                ON DELETE RESTRICT,
            CONSTRAINT fk_demo_workers_run_id_demo_runs
                FOREIGN KEY (run_id) REFERENCES demo_runs(run_id) ON DELETE RESTRICT
        )
    """)
    op.create_index("ix_demo_workers_run_id", "demo_workers", ["run_id"])
    op.execute("""
        CREATE TABLE demo_invocations (
            id UUID CONSTRAINT pk_demo_invocations PRIMARY KEY,
            attempt_id UUID NOT NULL,
            clock_domain VARCHAR(160) NOT NULL,
            CONSTRAINT fk_demo_invocations_attempt_id_attempt_leases
                FOREIGN KEY (attempt_id) REFERENCES attempt_leases(attempt_id)
                ON DELETE RESTRICT,
            CONSTRAINT ck_demo_invocations_clock_domain_nonempty
                CHECK (length(clock_domain) > 0)
        )
    """)
    op.create_index(
        "ix_demo_invocations_attempt_id", "demo_invocations", ["attempt_id"]
    )
    op.execute("""
        CREATE TABLE demo_samples (
            invocation_id UUID NOT NULL,
            sequence INTEGER NOT NULL,
            phase VARCHAR(8) NOT NULL,
            monotonic_ns BIGINT NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_demo_samples PRIMARY KEY (invocation_id, sequence),
            CONSTRAINT fk_demo_samples_invocation_id_demo_invocations
                FOREIGN KEY (invocation_id) REFERENCES demo_invocations(id)
                ON DELETE RESTRICT,
            CONSTRAINT ck_demo_samples_sequence_bound
                CHECK (sequence BETWEEN 0 AND 240),
            CONSTRAINT ck_demo_samples_monotonic_nonnegative CHECK (monotonic_ns >= 0),
            CONSTRAINT ck_demo_samples_phase_values CHECK
                ((sequence = 0 AND phase = 'START') OR
                 (sequence > 0 AND phase IN ('PULSE', 'FINISH'))),
            CONSTRAINT ck_demo_samples_recorded_finite CHECK (isfinite(recorded_at))
        )
    """)
    op.execute(
        "CREATE UNIQUE INDEX uq_demo_samples_finish ON demo_samples(invocation_id) "
        "WHERE phase = 'FINISH'"
    )
    op.execute("""
        CREATE FUNCTION dwe_reject_demo_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION USING ERRCODE='55000',
                MESSAGE='demo evidence is append-only';
        END; $$
    """)
    for table in ("demo_runs", "demo_workers", "demo_invocations", "demo_samples"):
        op.execute(
            f"CREATE TRIGGER {table}_append_only BEFORE UPDATE OR DELETE OR TRUNCATE "
            f"ON {table} FOR EACH STATEMENT "
            "EXECUTE FUNCTION dwe_reject_demo_mutation()"
        )


def downgrade() -> None:
    # Explicit destructive downgrade only; no demo command calls this.
    for table in ("demo_samples", "demo_invocations", "demo_workers", "demo_runs"):
        op.drop_table(table)
    op.execute("DROP FUNCTION dwe_reject_demo_mutation()")
