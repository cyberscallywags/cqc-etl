"""Connection utility functions"""

# Imports
import functools
import os
import time

import duckdb
from loguru import logger
from neo4j import AsyncDriver, AsyncGraphDatabase, GraphDatabase
from neo4j.exceptions import (
    AuthError,
    DatabaseUnavailable,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)
import psycopg
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from src.config import (
    INTERIM_DATA_DIR,
    neo4j_pwd,
    neo4j_uri,
    neo4j_user,
    pg_db,
    pg_host,
    pg_port,
    pg_pwd,
    pg_user,
)


class PostgresDB:
    """PostgreSQL connection functions."""
    def __init__(self):
        if not all([pg_host, pg_db, pg_user]):
            raise ValueError("LOCAL_PG_HOST, LOCAL_PG_DB, and LOCAL_PG_USER must be set in .env")

        self.host = pg_host
        self.dbname = pg_db
        self.user = pg_user
        self.password = pg_pwd
        self.port = pg_port

    def connect(self, autocommit: bool = False) -> psycopg.Connection:
        """Create synchronous PostgreSQL connection."""
        try:
            conn = psycopg.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                port=self.port,
                autocommit=autocommit,
            )
            logger.info(f"Connected to PostgreSQL database '{self.dbname}' at {self.host}:{self.port}")
            return conn
        except psycopg.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    @staticmethod
    def ensure_postgis(conn: psycopg.Connection) -> bool:
        """
        Enable PostGIS extension if installed on the server.
        Returns True if PostGIS is active, False if falling back to native POINT.
        """
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'postgis';"
            )
            available = cur.fetchone() is not None
            if available:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                conn.commit()
                logger.info("PostGIS extension enabled.")
                return True

            logger.warning(
                "PostGIS extension is not installed on the PostgreSQL server; "
                "using native PostgreSQL POINT for geometry columns."
            )
            return False


class AuraDB:
    """Neo4j AuraDB connection functions."""
    def __init__(self):
        if not all([neo4j_uri, neo4j_user, neo4j_pwd]):
            raise ValueError("neo4j_uri, neo4j_user, and neo4j_pwd must not be None")

        self.uri: str = neo4j_uri  # type: ignore
        self.auth: tuple[str, str] = (neo4j_user, neo4j_pwd)  # type: ignore
        self.driver = None

    # Sync connection
    def connect_sync(self):
        """Create sync driver."""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=self.auth)
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to AuraDB (sync)")
            return self.driver
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"Failed to connect to AuraDB: {e}")
            raise

    # Async connection
    async def connect_async(self) -> AsyncDriver:
        """Create async driver."""
        try:
            self.driver = AsyncGraphDatabase.driver(self.uri, auth=self.auth)
            async with self.driver.session() as session:
                await session.run("RETURN 1")
            logger.info("Connected to AuraDB (async)")
            return self.driver
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"Failed to connect to AuraDB: {e}")
            raise


def connect_duckdb():
    """Create/connect to local DuckDB database, register macros, and return connection."""
    try:
        os.makedirs(INTERIM_DATA_DIR, exist_ok=True)
        con = duckdb.connect(INTERIM_DATA_DIR / "cqc.duckdb")
        con.execute(
            """
            CREATE OR REPLACE MACRO cqc_uuid5(name) AS (
                concat(
                    substr(sha1(concat('cqc-etl:', name)), 1, 8), '-',
                    substr(sha1(concat('cqc-etl:', name)), 9, 4), '-5',
                    substr(sha1(concat('cqc-etl:', name)), 14, 3), '-a',
                    substr(sha1(concat('cqc-etl:', name)), 18, 3), '-',
                    substr(sha1(concat('cqc-etl:', name)), 21, 12)
                )::UUID
            );
            """
        )
        logger.info("Connected to CQC DuckDB database.")
        return con

    except duckdb.Error as e:
        logger.error(f"DuckDB connection failed: {e}")
        raise

    except OSError as e:
        logger.error(f"Failed to prepare data directory: {e}")
        raise


RETRYABLE_ERRORS = (
    # Neo4j / AuraDB
    ServiceUnavailable,
    TransientError,
    SessionExpired,
    DatabaseUnavailable,

    # HTTP / Network
    ConnectionError,
    RequestsConnectionError,
    TimeoutError,
    Timeout,
    HTTPError,  # Only retry on 5xx errors

    # OS-level
    OSError,  # Socket errors, file descriptor issues
)

def retry(max_attempts=3, backoff=1.5):
    """Retry function decorator with smart HTTP error handling."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            retry_count = 0

            while True:
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, dict):
                        result["_retry_count"] = retry_count
                    return result

                except HTTPError as exc:
                    # Only retry server errors (5xx), not client errors (4xx)
                    if exc.response is not None and 500 <= exc.response.status_code < 600:
                        if attempt >= max_attempts:
                            raise
                        # Proceed to retry logic below
                    else:
                        # Don't retry 4xx errors (bad request, auth, etc.)
                        raise

                except RETRYABLE_ERRORS as exc:
                    if attempt >= max_attempts:
                        raise

                    retry_count += 1
                    wait = backoff ** attempt

                    logger.warning(
                        f"Retryable error in {func.__name__}: {exc}. "
                        f"Retrying in {wait:.1f}s (attempt {attempt}/{max_attempts})"
                    )

                    time.sleep(wait)
                    attempt += 1

        return wrapper
    return decorator
