import duckdb


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            description     VARCHAR,
            created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_loads (
            id              VARCHAR PRIMARY KEY,
            dataset_id      VARCHAR NOT NULL REFERENCES datasets(id),
            version_number  INTEGER NOT NULL,
            description     VARCHAR,
            created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
            is_active       BOOLEAN NOT NULL DEFAULT true,
            UNIQUE (dataset_id, version_number)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sequences (
            id              VARCHAR PRIMARY KEY,
            dataset_id      VARCHAR NOT NULL REFERENCES datasets(id),
            load_id         VARCHAR NOT NULL REFERENCES data_loads(id),
            molecule_type   VARCHAR NOT NULL,
            name            VARCHAR,
            metadata        JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS chains (
            id              VARCHAR PRIMARY KEY,
            sequence_id     VARCHAR NOT NULL REFERENCES sequences(id),
            chain_id        VARCHAR NOT NULL,
            chain_index     INTEGER NOT NULL,
            sequence        VARCHAR NOT NULL,
            UNIQUE (sequence_id, chain_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id              VARCHAR PRIMARY KEY,
            sequence_id     VARCHAR NOT NULL REFERENCES sequences(id),
            load_id         VARCHAR NOT NULL REFERENCES data_loads(id),
            assay_name      VARCHAR NOT NULL,
            value           DOUBLE NOT NULL,
            metadata        JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS models (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            version         VARCHAR NOT NULL,
            model_type      VARCHAR NOT NULL,
            config          JSON NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
            UNIQUE (name, version)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id              VARCHAR PRIMARY KEY,
            task_type       VARCHAR NOT NULL,
            status          VARCHAR NOT NULL DEFAULT 'pending',
            created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
            started_at      TIMESTAMP,
            completed_at    TIMESTAMP,
            error_message   VARCHAR,
            result_ref      VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              VARCHAR PRIMARY KEY,
            model_id        VARCHAR NOT NULL REFERENCES models(id),
            task_id         VARCHAR REFERENCES tasks(id),
            dataset_id      VARCHAR REFERENCES datasets(id),
            data_version    INTEGER,
            scoring_method  VARCHAR NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS prediction_scores (
            id              VARCHAR PRIMARY KEY,
            prediction_id   VARCHAR NOT NULL REFERENCES predictions(id),
            sequence_id     VARCHAR NOT NULL REFERENCES sequences(id),
            score           DOUBLE NOT NULL,
            details         JSON
        )
    """)
