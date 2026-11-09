"""Load transformed DuckDB tables into PostgreSQL (Source of Truth)."""

# Imports
import datetime

from loguru import logger
import psycopg
from psycopg import sql

from src.utils.connectors import PostgresDB, connect_duckdb

# Ordered table definitions for topological FK loading
# Each tuple: (table_name, columns_from_duckdb, has_geom)
ENTITY_TABLES_ORDERED: list[tuple[str, list[str], bool]] = [
    # 1. Independent Lookups
    ("org_types", ["_id", "name"], False),
    ("inspection_categories", ["_id", "name"], False),
    ("inspection_directorates", ["_id", "name"], False),
    ("service_types", ["_id", "name"], False),
    ("service_user_bands", ["_id", "name"], False),
    ("regulated_activities", ["_id", "name"], False),
    ("service_groups", ["_id", "name"], False),
    ("report_types", ["_id", "name"], False),
    ("brands", ["_id", "name"], False),

    # 2. Geospatial Hierarchy (top-down)
    ("regions", ["_id", "name"], False),
    ("nhser", ["_id", "name"], False),
    ("icbs", ["_id", "name"], False),
    ("counties", ["_id", "name", "rgncd"], False),
    (
        "local_authorities",
        ["_id", "name", "ctycd", "rgncd", "icbcd", "nhsercd", "lat", "long"],
        True,
    ),
    ("postcodes", ["_id", "ladcd", "lat", "long"], True),

    # 3. Core CQC Entities
    (
        "providers",
        [
            "_id",
            "name",
            "org_type_id",
            "inspection_directorate_id",
            "primary_inspection_category_id",
            "address",
            "city",
            "postcode",
            "parliamentary_constituency",
            "lat",
            "long",
            "phone",
            "url",
            "hsca_start_date",
        ],
        True,
    ),
    (
        "locations",
        [
            "_id",
            "ods_code",
            "name",
            "org_type_id",
            "dormant",
            "is_care_home",
            "care_home_bed_count",
            "inspection_directorate_id",
            "primary_inspection_category_id",
            "address",
            "city",
            "postcode",
            "parliamentary_constituency",
            "lat",
            "long",
            "phone",
            "url",
            "provider_id",
            "brand_id",
            "dual_registered",
            "dual_registration_pid",
            "hsca_start_date",
        ],
        True,
    ),

    # 4. Time-Series / Observation Tables
    (
        "location_ratings",
        [
            "_id",
            "location_id",
            "report_type_id",
            "service_group_id",
            "rating_safe",
            "rating_effective",
            "rating_caring",
            "rating_responsive",
            "rating_well_led",
            "rating_overall",
            "inherited_rating",
            "publication_date",
        ],
        False,
    ),
    (
        "provider_ratings",
        [
            "_id",
            "provider_id",
            "report_type_id",
            "service_group_id",
            "rating_safe",
            "rating_effective",
            "rating_caring",
            "rating_responsive",
            "rating_well_led",
            "rating_overall",
            "inherited_rating",
            "publication_date",
        ],
        False,
    ),
    (
        "lad_populations",
        [
            "_id",
            "ladcd",
            "year",
            "total_population",
            "age_50_54",
            "age_55_59",
            "age_60_64",
            "age_65_69",
            "age_70_74",
            "age_75_79",
            "age_80_84",
            "age_85_89",
            "age_90_plus",
        ],
        False,
    ),
    (
        "lad_life_expectancy",
        [
            "_id",
            "ladcd",
            "start_year",
            "interval",
            "age_50_54",
            "age_55_59",
            "age_60_64",
            "age_65_69",
            "age_70_74",
            "age_75_79",
            "age_80_84",
            "age_85_89",
            "age_90_plus",
        ],
        False,
    ),
]

BRIDGE_TABLES: list[tuple[str, str, str]] = [
    ("location_service_types", "location_id", "service_type_id"),
    ("location_service_user_bands", "location_id", "service_user_band_id"),
    ("location_regulated_activities", "location_id", "regulated_activity_id"),
]


def init_postgres_schema(conn: psycopg.Connection, has_postgis: bool):
    """Create all PostgreSQL tables, foreign keys, and indexes if they do not exist."""
    geom_type = "GEOMETRY(Point, 4326)" if has_postgis else "POINT"

    ddl_statements = [
        # Lookups (UUIDv5 PK)
        """
        CREATE TABLE IF NOT EXISTS org_types (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS inspection_categories (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS inspection_directorates (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS service_types (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS service_user_bands (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS regulated_activities (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS service_groups (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS report_types (
            _id UUID PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS brands (
            _id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,

        # Geospatial Hierarchy
        """
        CREATE TABLE IF NOT EXISTS regions (
            _id TEXT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS nhser (
            _id TEXT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS icbs (
            _id TEXT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS counties (
            _id TEXT PRIMARY KEY,
            name TEXT,
            rgncd TEXT REFERENCES regions(_id),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS local_authorities (
            _id TEXT PRIMARY KEY,
            name TEXT,
            ctycd TEXT REFERENCES counties(_id),
            rgncd TEXT REFERENCES regions(_id),
            icbcd TEXT REFERENCES icbs(_id),
            nhsercd TEXT REFERENCES nhser(_id),
            lat DOUBLE PRECISION,
            long DOUBLE PRECISION,
            geom {geom_type},
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS postcodes (
            _id TEXT PRIMARY KEY,
            ladcd TEXT REFERENCES local_authorities(_id),
            lat DOUBLE PRECISION,
            long DOUBLE PRECISION,
            geom {geom_type},
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,

        # Core Entities
        f"""
        CREATE TABLE IF NOT EXISTS providers (
            _id TEXT PRIMARY KEY,
            name TEXT,
            org_type_id UUID REFERENCES org_types(_id),
            inspection_directorate_id UUID REFERENCES inspection_directorates(_id),
            primary_inspection_category_id UUID REFERENCES inspection_categories(_id),
            address TEXT,
            city TEXT,
            postcode TEXT REFERENCES postcodes(_id),
            parliamentary_constituency TEXT,
            lat DOUBLE PRECISION,
            long DOUBLE PRECISION,
            geom {geom_type},
            phone TEXT,
            url TEXT,
            hsca_start_date DATE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS locations (
            _id TEXT PRIMARY KEY,
            ods_code TEXT,
            name TEXT,
            org_type_id UUID REFERENCES org_types(_id),
            dormant BOOLEAN,
            is_care_home BOOLEAN,
            care_home_bed_count INTEGER,
            inspection_directorate_id UUID REFERENCES inspection_directorates(_id),
            primary_inspection_category_id UUID REFERENCES inspection_categories(_id),
            address TEXT,
            city TEXT,
            postcode TEXT REFERENCES postcodes(_id),
            parliamentary_constituency TEXT,
            lat DOUBLE PRECISION,
            long DOUBLE PRECISION,
            geom {geom_type},
            phone TEXT,
            url TEXT,
            provider_id TEXT REFERENCES providers(_id),
            brand_id TEXT REFERENCES brands(_id),
            dual_registered BOOLEAN,
            dual_registration_pid TEXT,
            hsca_start_date DATE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,

        # Bridge Tables
        """
        CREATE TABLE IF NOT EXISTS location_service_types (
            location_id TEXT NOT NULL REFERENCES locations(_id) ON DELETE CASCADE,
            service_type_id UUID NOT NULL REFERENCES service_types(_id) ON DELETE CASCADE,
            PRIMARY KEY (location_id, service_type_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS location_service_user_bands (
            location_id TEXT NOT NULL REFERENCES locations(_id) ON DELETE CASCADE,
            service_user_band_id UUID NOT NULL REFERENCES service_user_bands(_id) ON DELETE CASCADE,
            PRIMARY KEY (location_id, service_user_band_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS location_regulated_activities (
            location_id TEXT NOT NULL REFERENCES locations(_id) ON DELETE CASCADE,
            regulated_activity_id UUID NOT NULL REFERENCES regulated_activities(_id) ON DELETE CASCADE,
            PRIMARY KEY (location_id, regulated_activity_id)
        );
        """,

        # Time-Series Tables
        """
        CREATE TABLE IF NOT EXISTS location_ratings (
            _id UUID PRIMARY KEY,
            location_id TEXT NOT NULL REFERENCES locations(_id) ON DELETE CASCADE,
            report_type_id UUID REFERENCES report_types(_id),
            service_group_id UUID REFERENCES service_groups(_id),
            rating_safe TEXT,
            rating_effective TEXT,
            rating_caring TEXT,
            rating_responsive TEXT,
            rating_well_led TEXT,
            rating_overall TEXT,
            inherited_rating BOOLEAN,
            publication_date DATE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE NULLS NOT DISTINCT (location_id, service_group_id, report_type_id, publication_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS provider_ratings (
            _id UUID PRIMARY KEY,
            provider_id TEXT NOT NULL REFERENCES providers(_id) ON DELETE CASCADE,
            report_type_id UUID REFERENCES report_types(_id),
            service_group_id UUID REFERENCES service_groups(_id),
            rating_safe TEXT,
            rating_effective TEXT,
            rating_caring TEXT,
            rating_responsive TEXT,
            rating_well_led TEXT,
            rating_overall TEXT,
            inherited_rating BOOLEAN,
            publication_date DATE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE NULLS NOT DISTINCT (provider_id, service_group_id, report_type_id, publication_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS lad_populations (
            _id UUID PRIMARY KEY,
            ladcd TEXT NOT NULL REFERENCES local_authorities(_id) ON DELETE CASCADE,
            year INTEGER NOT NULL,
            total_population INTEGER,
            age_50_54 INTEGER,
            age_55_59 INTEGER,
            age_60_64 INTEGER,
            age_65_69 INTEGER,
            age_70_74 INTEGER,
            age_75_79 INTEGER,
            age_80_84 INTEGER,
            age_85_89 INTEGER,
            age_90_plus INTEGER,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (ladcd, year)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS lad_life_expectancy (
            _id UUID PRIMARY KEY,
            ladcd TEXT NOT NULL REFERENCES local_authorities(_id) ON DELETE CASCADE,
            start_year INTEGER NOT NULL,
            interval TEXT NOT NULL,
            age_50_54 DOUBLE PRECISION[],
            age_55_59 DOUBLE PRECISION[],
            age_60_64 DOUBLE PRECISION[],
            age_65_69 DOUBLE PRECISION[],
            age_70_74 DOUBLE PRECISION[],
            age_75_79 DOUBLE PRECISION[],
            age_80_84 DOUBLE PRECISION[],
            age_85_89 DOUBLE PRECISION[],
            age_90_plus DOUBLE PRECISION[],
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (ladcd, interval)
        );
        """,

        # Indexes for fast relational & graph projection queries
        "CREATE INDEX IF NOT EXISTS idx_locations_provider_id ON locations(provider_id);",
        "CREATE INDEX IF NOT EXISTS idx_locations_postcode ON locations(postcode);",
        "CREATE INDEX IF NOT EXISTS idx_providers_postcode ON providers(postcode);",
        "CREATE INDEX IF NOT EXISTS idx_postcodes_ladcd ON postcodes(ladcd);",
        "CREATE INDEX IF NOT EXISTS idx_loc_ratings_ts ON location_ratings(location_id, report_type_id, service_group_id, publication_date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_prov_ratings_ts ON provider_ratings(provider_id, report_type_id, service_group_id, publication_date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_lad_pop_ts ON lad_populations(ladcd, year DESC);",
        "CREATE INDEX IF NOT EXISTS idx_lad_le_ts ON lad_life_expectancy(ladcd, start_year DESC);",
        "CREATE INDEX IF NOT EXISTS idx_locations_geom ON locations USING GIST(geom);",
        "CREATE INDEX IF NOT EXISTS idx_providers_geom ON providers USING GIST(geom);",
        "CREATE INDEX IF NOT EXISTS idx_postcodes_geom ON postcodes USING GIST(geom);",
        "CREATE INDEX IF NOT EXISTS idx_local_authorities_geom ON local_authorities USING GIST(geom);",
    ]

    with conn.cursor() as cur:
        for stmt in ddl_statements:
            cur.execute(stmt)
    conn.commit()
    logger.success("PostgreSQL schema and indexes initialized.")


def _geom_sql_expr(has_postgis: bool, table_alias: str = "s") -> str:
    """Return SQL expression to build Point geometry from (long, lat)."""
    if has_postgis:
        return (
            f"CASE WHEN {table_alias}.long IS NOT NULL AND {table_alias}.lat IS NOT NULL "
            f"THEN ST_SetSRID(ST_MakePoint({table_alias}.long, {table_alias}.lat), 4326) "
            f"ELSE NULL END"
        )
    return (
        f"CASE WHEN {table_alias}.long IS NOT NULL AND {table_alias}.lat IS NOT NULL "
        f"THEN point({table_alias}.long, {table_alias}.lat) "
        f"ELSE NULL END"
    )


def upsert_entity_table(
    duck_con,
    pg_conn: psycopg.Connection,
    table_name: str,
    columns: list[str],
    has_geom: bool,
    has_postgis: bool,
    ts: str,
):
    """Bulk-copy a DuckDB table into a PostgreSQL temp table and upsert into the target table."""
    col_list_str = ", ".join(f'"{c}"' for c in columns)
    rows = duck_con.execute(f"SELECT {col_list_str} FROM {table_name}").fetchall()

    if not rows:
        logger.warning(f"No rows in DuckDB table '{table_name}', skipping.")
        return 0

    tmp_table = f"tmp_{table_name}"
    copy_cols = [*columns, "updated_at"]

    # Build INSERT ... ON CONFLICT (_id) DO UPDATE statement
    insert_cols = [*copy_cols, *(["geom"] if has_geom else [])]
    select_exprs = [f's."{c}"' for c in copy_cols]
    if has_geom:
        select_exprs.append(f"{_geom_sql_expr(has_postgis, 's')} AS geom")

    non_pk_cols = [c for c in insert_cols if c != "_id"]
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)

    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(tmp_table),
                sql.Identifier(table_name),
            )
        )

        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(tmp_table),
            sql.SQL(", ").join(sql.Identifier(c) for c in copy_cols),
        )
        with cur.copy(copy_stmt) as copy:
            for row in rows:
                copy.write_row((*row, ts))

        upsert_sql = f"""
            INSERT INTO "{table_name}" ({", ".join(f'"{c}"' for c in insert_cols)})
            SELECT {", ".join(select_exprs)}
            FROM "{tmp_table}" s
            ON CONFLICT ("_id") DO UPDATE SET
                {update_set};
        """
        cur.execute(upsert_sql)

    pg_conn.commit()
    logger.success(f"Upserted {len(rows):,} rows into PostgreSQL table '{table_name}'.")
    return len(rows)


def upsert_bridge_table(
    duck_con,
    pg_conn: psycopg.Connection,
    table_name: str,
    source_col: str,
    target_col: str,
):
    """Bulk-sync a many-to-many bridge table from DuckDB to PostgreSQL."""
    rows = duck_con.execute(
        f'SELECT "{source_col}", "{target_col}" FROM {table_name}'
    ).fetchall()

    if not rows:
        logger.warning(f"No rows in DuckDB bridge table '{table_name}', skipping.")
        return 0

    tmp_table = f"tmp_{table_name}"
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(
                sql.Identifier(tmp_table),
                sql.Identifier(table_name),
            )
        )

        copy_stmt = sql.SQL("COPY {} ({}, {}) FROM STDIN").format(
            sql.Identifier(tmp_table),
            sql.Identifier(source_col),
            sql.Identifier(target_col),
        )
        with cur.copy(copy_stmt) as copy:
            for row in rows:
                copy.write_row(row)

        # Remove stale links for updated sources and insert current links
        cur.execute(
            sql.SQL(
                """
                DELETE FROM {target_tbl}
                WHERE {src_col} IN (SELECT DISTINCT {src_col} FROM {tmp_tbl});
                """
            ).format(
                target_tbl=sql.Identifier(table_name),
                src_col=sql.Identifier(source_col),
                tmp_tbl=sql.Identifier(tmp_table),
            )
        )
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {target_tbl} ({src_col}, {tgt_col})
                SELECT {src_col}, {tgt_col} FROM {tmp_tbl}
                ON CONFLICT ({src_col}, {tgt_col}) DO NOTHING;
                """
            ).format(
                target_tbl=sql.Identifier(table_name),
                src_col=sql.Identifier(source_col),
                tgt_col=sql.Identifier(target_col),
                tmp_tbl=sql.Identifier(tmp_table),
            )
        )

    pg_conn.commit()
    logger.success(f"Synced {len(rows):,} links in PostgreSQL bridge table '{table_name}'.")
    return len(rows)


def sync_duckdb_to_postgres(duck_con=None, timestamp: str | None = None):
    """Initialize PostgreSQL schema and upsert all transformed DuckDB tables."""
    close_duck = False
    if duck_con is None:
        duck_con = connect_duckdb()
        close_duck = True

    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    pg_db = PostgresDB()
    with pg_db.connect() as pg_conn:
        has_postgis = pg_db.ensure_postgis(pg_conn)
        init_postgres_schema(pg_conn, has_postgis=has_postgis)

        for table_name, cols, has_geom in ENTITY_TABLES_ORDERED:
            upsert_entity_table(
                duck_con=duck_con,
                pg_conn=pg_conn,
                table_name=table_name,
                columns=cols,
                has_geom=has_geom,
                has_postgis=has_postgis,
                ts=timestamp,
            )

        for bridge_name, src_col, tgt_col in BRIDGE_TABLES:
            upsert_bridge_table(
                duck_con=duck_con,
                pg_conn=pg_conn,
                table_name=bridge_name,
                source_col=src_col,
                target_col=tgt_col,
            )

    if close_duck:
        duck_con.close()

    logger.success("All tables successfully synced to PostgreSQL Source of Truth.")


if __name__ == "__main__":
    sync_duckdb_to_postgres()
