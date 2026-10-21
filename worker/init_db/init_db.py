from sqlalchemy import create_engine, text


# --------------------------------------------------
# Database connection details
# --------------------------------------------------

DB_USER = "dm_postgres"
DB_PASSWORD = "dm_postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "dm_postgres"


engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


try:

    with engine.begin() as connection:

        print("Connecting to PostgreSQL...", flush=True)

        # --------------------------------------------------
        # Create schema
        # --------------------------------------------------

        connection.execute(
            text("""
                CREATE SCHEMA IF NOT EXISTS raw;
            """)
        )

        print("Database initialization completed successfully.")


except Exception as error:

    print(
        "Database initialization failed:",
        flush=True
    )

    print(error, flush=True)

    raise