"""DuckDB data transformation queries"""

# Imports
import json

from loguru import logger

from src.config import SCHEMAS_DIR

# pylint: disable=C0301


# FUNCTIONS

def run_queries(con, queries=None, select: str = ""):
    """
    Run a list of DuckDB queries.
    Optionally choose from a predefined set of ETL pipeline queries.
    """
    query_map = {
        "": queries,
        "transform": TRANSFORM_QUERIES,
        "init_ratings": INITIALISE_RATINGS_QUERIES,
    }

    if select not in query_map:
        raise ValueError(
            "Select parameter must either be 'transform', 'init_ratings' or left blank."
        )

    selected_queries = query_map[select]
    if selected_queries is None:
        raise ValueError("No queries provided and no predefined query set selected.")

    n_queries = len(selected_queries)

    for i, query in enumerate(selected_queries, start=1):
        logger.info(f"Running queries: {i} of {n_queries}")
        con.execute(query)


def load_schema(schema: str):
    """Load a schema from the schemas folder."""
    schema = schema.split(".")[0]
    with open(SCHEMAS_DIR / f"{schema}.json", "r", encoding="utf-8") as f:
        return json.load(f)


# QUERY PIPELINES

TRANSFORM_QUERIES = [
    # 1. Raw staging tables in DuckDB
    "CREATE OR REPLACE TABLE onspd_raw AS SELECT * FROM onspd_df",
    "CREATE OR REPLACE TABLE la_districts_raw AS SELECT * FROM la_districts_df",
    "CREATE OR REPLACE TABLE counties_raw AS SELECT * FROM counties_df",
    "CREATE OR REPLACE TABLE icbs_raw AS SELECT * FROM icbs_df",
    "CREATE OR REPLACE TABLE regions_raw AS SELECT * FROM regions_df",
    "CREATE OR REPLACE TABLE nhser_raw AS SELECT * FROM nhser_df",
    "CREATE OR REPLACE TABLE locations_raw AS SELECT * FROM locations_df",
    "CREATE OR REPLACE TABLE location_ratings_raw AS SELECT * FROM location_ratings_df",
    "CREATE OR REPLACE TABLE provider_ratings_raw AS SELECT * FROM provider_ratings_df",
    "CREATE OR REPLACE TABLE past_location_ratings AS SELECT * FROM past_location_ratings_df",
    "CREATE OR REPLACE TABLE past_provider_ratings AS SELECT * FROM past_provider_ratings_df",
    "CREATE OR REPLACE TABLE lad_pop_raw AS SELECT * FROM lad_pop_df",
    "CREATE OR REPLACE TABLE lad_life_exp_raw AS SELECT * FROM lad_life_exp_df",

    # 2. Intermediate ratings staging (before FK resolution)
    """
    CREATE OR REPLACE TABLE location_ratings_staged AS
    WITH latest_base AS (
        SELECT
            locationId,
            serviceGroup,
            reportType,
            inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM location_ratings_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY locationId, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    latest_pivoted AS (
        SELECT *
        FROM (
            SELECT
                locationId,
                serviceGroup,
                domain,
                latestRating
            FROM location_ratings_raw
        )
        PIVOT (
            FIRST(latestRating)
            FOR domain IN (
                'Safe' AS ratingSafe,
                'Effective' AS ratingEffective,
                'Caring' AS ratingCaring,
                'Responsive' AS ratingResponsive,
                'Well-led' AS ratingWellLed,
                'Overall' AS ratingOverall
            )
        )
    ),
    latest_combined AS (
        SELECT
            b.locationId,
            b.reportType,
            b.serviceGroup,
            p.ratingSafe,
            p.ratingEffective,
            p.ratingCaring,
            p.ratingResponsive,
            p.ratingWellLed,
            p.ratingOverall,
            b.inheritedRating,
            b.publicationDate
        FROM latest_base b
        LEFT JOIN latest_pivoted p USING (locationId, serviceGroup)
    ),
    all_ratings AS (
        SELECT
            locationId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM latest_combined
        UNION ALL
        SELECT
            locationId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM past_location_ratings
    )
    SELECT *
    FROM all_ratings
    WHERE publicationDate IS NOT NULL
      AND COALESCE(
          ratingSafe, ratingEffective, ratingCaring,
          ratingResponsive, ratingWellLed, ratingOverall
      ) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY locationId, serviceGroup, publicationDate, reportType
        ORDER BY publicationDate DESC
    ) = 1;
    """,

    """
    CREATE OR REPLACE TABLE provider_ratings_staged AS
    WITH latest_base AS (
        SELECT
            providerId,
            serviceGroup,
            reportType,
            inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM provider_ratings_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY providerId, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    latest_pivoted AS (
        SELECT *
        FROM (
            SELECT
                providerId,
                serviceGroup,
                domain,
                latestRating
            FROM provider_ratings_raw
        )
        PIVOT (
            FIRST(latestRating)
            FOR domain IN (
                'Safe' AS ratingSafe,
                'Effective' AS ratingEffective,
                'Caring' AS ratingCaring,
                'Responsive' AS ratingResponsive,
                'Well-led' AS ratingWellLed,
                'Overall' AS ratingOverall
            )
        )
    ),
    latest_combined AS (
        SELECT
            b.providerId,
            b.reportType,
            b.serviceGroup,
            p.ratingSafe,
            p.ratingEffective,
            p.ratingCaring,
            p.ratingResponsive,
            p.ratingWellLed,
            p.ratingOverall,
            b.inheritedRating,
            b.publicationDate
        FROM latest_base b
        LEFT JOIN latest_pivoted p USING (providerId, serviceGroup)
    ),
    all_ratings AS (
        SELECT
            providerId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM latest_combined
        UNION ALL
        SELECT
            providerId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM past_provider_ratings
    )
    SELECT *
    FROM all_ratings
    WHERE publicationDate IS NOT NULL
      AND COALESCE(
          ratingSafe, ratingEffective, ratingCaring,
          ratingResponsive, ratingWellLed, ratingOverall
      ) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY providerId, serviceGroup, publicationDate, reportType
        ORDER BY publicationDate DESC
    ) = 1;
    """,

    # 3. Lookup Tables (with deterministic UUIDv5 primary keys)
    """
    CREATE OR REPLACE TABLE service_user_bands AS
    WITH distinct_subs AS (
        SELECT DISTINCT trim(serviceUserBand) AS name
        FROM locations_raw
        CROSS JOIN UNNEST(serviceUserBands) AS t(serviceUserBand)
        WHERE serviceUserBand IS NOT NULL
          AND trim(serviceUserBand) NOT IN ('', 'nan')
    )
    SELECT
        cqc_uuid5('service_user_bands:' || name) AS _id,
        name
    FROM distinct_subs
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE service_types AS
    WITH distinct_types AS (
        SELECT DISTINCT trim(serviceType) AS name
        FROM locations_raw
        CROSS JOIN UNNEST(serviceTypes) AS t(serviceType)
        WHERE serviceType IS NOT NULL
          AND trim(serviceType) NOT IN ('', 'nan')
    )
    SELECT
        cqc_uuid5('service_types:' || name) AS _id,
        name
    FROM distinct_types
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE regulated_activities AS
    WITH distinct_acts AS (
        SELECT DISTINCT trim(regulatedActivity) AS name
        FROM locations_raw
        CROSS JOIN UNNEST(regulatedActivities) AS t(regulatedActivity)
        WHERE regulatedActivity IS NOT NULL
          AND trim(regulatedActivity) NOT IN ('', 'nan')
    )
    SELECT
        cqc_uuid5('regulated_activities:' || name) AS _id,
        name
    FROM distinct_acts
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE service_groups AS
    WITH raw AS (
        SELECT trim(serviceGroup) AS name FROM location_ratings_staged WHERE serviceGroup IS NOT NULL AND trim(serviceGroup) <> ''
        UNION
        SELECT trim(serviceGroup) AS name FROM provider_ratings_staged WHERE serviceGroup IS NOT NULL AND trim(serviceGroup) <> ''
    )
    SELECT
        cqc_uuid5('service_groups:' || name) AS _id,
        name
    FROM (SELECT DISTINCT name FROM raw)
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE report_types AS
    WITH raw AS (
        SELECT trim(reportType) AS name FROM location_ratings_staged WHERE reportType IS NOT NULL AND trim(reportType) <> ''
        UNION
        SELECT trim(reportType) AS name FROM provider_ratings_staged WHERE reportType IS NOT NULL AND trim(reportType) <> ''
    )
    SELECT
        cqc_uuid5('report_types:' || name) AS _id,
        name
    FROM (SELECT DISTINCT name FROM raw)
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE org_types AS
    WITH raw AS (
        SELECT trim(type) AS name FROM locations_raw WHERE type IS NOT NULL AND trim(type) <> ''
        UNION
        SELECT trim(providerType) AS name FROM locations_raw WHERE providerType IS NOT NULL AND trim(providerType) <> ''
    )
    SELECT
        cqc_uuid5('org_types:' || name) AS _id,
        name
    FROM (SELECT DISTINCT name FROM raw)
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE inspection_categories AS
    WITH raw AS (
        SELECT trim(primaryInspectionCategory) AS name FROM locations_raw WHERE primaryInspectionCategory IS NOT NULL AND trim(primaryInspectionCategory) NOT IN ('', '-')
        UNION
        SELECT trim(providerPrimaryInspectionCategory) AS name FROM locations_raw WHERE providerPrimaryInspectionCategory IS NOT NULL AND trim(providerPrimaryInspectionCategory) NOT IN ('', '-')
    )
    SELECT
        cqc_uuid5('inspection_categories:' || name) AS _id,
        name
    FROM (SELECT DISTINCT name FROM raw)
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE inspection_directorates AS
    WITH raw AS (
        SELECT trim(inspectionDirectorate) AS name FROM locations_raw WHERE inspectionDirectorate IS NOT NULL AND trim(inspectionDirectorate) NOT IN ('', '-', 'Unspecified')
        UNION
        SELECT trim(providerInspectionDirectorate) AS name FROM locations_raw WHERE providerInspectionDirectorate IS NOT NULL AND trim(providerInspectionDirectorate) NOT IN ('', '-', 'Unspecified')
    )
    SELECT
        cqc_uuid5('inspection_directorates:' || name) AS _id,
        name
    FROM (SELECT DISTINCT name FROM raw)
    ORDER BY name;
    """,

    """
    CREATE OR REPLACE TABLE brands AS
    WITH brands_clean AS (
        SELECT
            trim(brandId) AS _id,
            regexp_replace(trim(brandName), '^BRAND ', '') AS name
        FROM locations_raw
        WHERE brandId IS NOT NULL AND trim(brandId) NOT IN ('', '-')
    )
    SELECT _id, name
    FROM brands_clean
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY name NULLS LAST) = 1
    ORDER BY _id;
    """,

    # 4. Geospatial Hierarchy
    """
    CREATE OR REPLACE TABLE onspd_filtered AS
    WITH postcodes_unique AS (
        SELECT trim(postcode) AS postcode FROM locations_raw WHERE postcode IS NOT NULL AND trim(postcode) <> ''
        UNION
        SELECT trim(providerPostcode) AS postcode FROM locations_raw WHERE providerPostcode IS NOT NULL AND trim(providerPostcode) <> ''
    )
    SELECT o.*
    FROM onspd_raw o
    JOIN postcodes_unique u
        ON trim(o.pcds) = u.postcode;
    """,

    """
    CREATE OR REPLACE TABLE onspd AS
    SELECT f.*,
        lad.name AS ladnm,
        lad.lat AS lad_lat,
        lad.long AS lad_long,
        cty.name AS ctynm,
        icb.name AS icbnm,
        rgn.name AS rgnnm,
        nhs.name AS nhsernm
    FROM onspd_filtered f
    LEFT JOIN la_districts_raw lad ON f.ladcd = lad.code
    LEFT JOIN counties_raw cty ON f.ctycd = cty.code
    LEFT JOIN icbs_raw icb ON f.icbcd = icb.code
    LEFT JOIN regions_raw rgn ON f.rgncd = rgn.code
    LEFT JOIN nhser_raw nhs ON f.nhsercd = nhs.code;
    """,

    """
    CREATE OR REPLACE TABLE regions AS
    SELECT
        rgncd AS _id,
        rgnnm AS name
    FROM onspd
    WHERE rgncd IS NOT NULL AND trim(rgncd) <> ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rgncd ORDER BY rgnnm NULLS LAST) = 1;
    """,

    """
    CREATE OR REPLACE TABLE nhser AS
    SELECT
        nhsercd AS _id,
        nhsernm AS name
    FROM onspd
    WHERE nhsercd IS NOT NULL AND trim(nhsercd) <> ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY nhsercd ORDER BY nhsernm NULLS LAST) = 1;
    """,

    """
    CREATE OR REPLACE TABLE icbs AS
    SELECT
        icbcd AS _id,
        icbnm AS name
    FROM onspd
    WHERE icbcd IS NOT NULL AND trim(icbcd) <> ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icbcd ORDER BY icbnm NULLS LAST) = 1;
    """,

    """
    CREATE OR REPLACE TABLE counties AS
    SELECT
        c.ctycd AS _id,
        c.ctynm AS name,
        r._id AS rgncd
    FROM onspd c
    LEFT JOIN regions r ON c.rgncd = r._id
    WHERE c.ctycd IS NOT NULL AND trim(c.ctycd) <> ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ctycd ORDER BY c.ctynm NULLS LAST) = 1;
    """,

    """
    CREATE OR REPLACE TABLE local_authorities AS
    SELECT
        la.ladcd AS _id,
        la.ladnm AS name,
        cty._id AS ctycd,
        rgn._id AS rgncd,
        icb._id AS icbcd,
        nhs._id AS nhsercd,
        TRY_CAST(la.lad_lat AS DOUBLE) AS lat,
        TRY_CAST(la.lad_long AS DOUBLE) AS long
    FROM onspd la
    LEFT JOIN counties cty ON la.ctycd = cty._id
    LEFT JOIN regions rgn ON la.rgncd = rgn._id
    LEFT JOIN icbs icb ON la.icbcd = icb._id
    LEFT JOIN nhser nhs ON la.nhsercd = nhs._id
    WHERE la.ladcd IS NOT NULL AND trim(la.ladcd) <> ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY la.ladcd ORDER BY la.ladnm NULLS LAST) = 1;
    """,

    """
    CREATE OR REPLACE TABLE postcodes AS
    WITH onspd_pc AS (
        SELECT
            trim(p.pcds) AS _id,
            la._id AS ladcd,
            TRY_CAST(p.lat AS DOUBLE) AS lat,
            TRY_CAST(p.long AS DOUBLE) AS long
        FROM onspd p
        LEFT JOIN local_authorities la ON p.ladcd = la._id
        WHERE p.pcds IS NOT NULL AND trim(p.pcds) <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(p.pcds) ORDER BY la._id NULLS LAST) = 1
    ),
    missing_pc AS (
        SELECT DISTINCT trim(pc) AS _id
        FROM (
            SELECT postcode AS pc FROM locations_raw WHERE postcode IS NOT NULL AND trim(postcode) NOT IN ('', '-')
            UNION
            SELECT providerPostcode AS pc FROM locations_raw WHERE providerPostcode IS NOT NULL AND trim(providerPostcode) NOT IN ('', '-')
        )
        WHERE trim(pc) NOT IN (SELECT _id FROM onspd_pc)
    )
    SELECT _id, ladcd, lat, long FROM onspd_pc
    UNION ALL
    SELECT _id, NULL AS ladcd, NULL AS lat, NULL AS long FROM missing_pc;
    """,

    # 5. Core Entities (Providers & Locations with FKs resolved)
    """
    CREATE OR REPLACE TABLE providers AS
    WITH raw_dedup AS (
        SELECT *
        FROM locations_raw
        WHERE providerId IS NOT NULL AND trim(providerId) NOT IN ('', '-')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(providerId) ORDER BY providerName NULLS LAST) = 1
    )
    SELECT
        trim(r.providerId) AS _id,
        r.providerName AS name,
        ot._id AS org_type_id,
        id_dir._id AS inspection_directorate_id,
        ic._id AS primary_inspection_category_id,
        r.provideraddress AS address,
        r.providercity AS city,
        pc._id AS postcode,
        r.providerParliamentaryConstituency AS parliamentary_constituency,
        TRY_CAST(r.providerLat AS DOUBLE) AS lat,
        TRY_CAST(r.providerLong AS DOUBLE) AS long,
        CASE
            WHEN r.providerPhone IS NULL OR trim(CAST(r.providerPhone AS VARCHAR)) IN ('', '-') THEN NULL
            ELSE '0' || split_part(CAST(r.providerPhone AS VARCHAR), '.', 1)
        END AS phone,
        NULLIF(trim(r.providerUrl), '') AS url,
        TRY_CAST(r.providerHscaStartDate AS DATE) AS hsca_start_date
    FROM raw_dedup r
    LEFT JOIN org_types ot ON trim(r.providerType) = ot.name
    LEFT JOIN inspection_directorates id_dir ON trim(r.providerInspectionDirectorate) = id_dir.name
    LEFT JOIN inspection_categories ic ON trim(r.providerPrimaryInspectionCategory) = ic.name
    LEFT JOIN postcodes pc ON trim(r.providerPostcode) = pc._id;
    """,

    """
    CREATE OR REPLACE TABLE locations AS
    WITH raw_dedup AS (
        SELECT *
        FROM locations_raw
        WHERE _id IS NOT NULL AND trim(_id) <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(_id) ORDER BY name NULLS LAST) = 1
    )
    SELECT
        trim(r._id) AS _id,
        NULLIF(trim(r.odsCode), '') AS ods_code,
        r.name AS name,
        ot._id AS org_type_id,
        CASE
            WHEN upper(trim(r.dormant)) = 'Y' THEN TRUE
            WHEN upper(trim(r.dormant)) = 'N' THEN FALSE
            ELSE NULL
        END AS dormant,
        CASE
            WHEN upper(trim(r.isCareHome)) = 'Y' THEN TRUE
            WHEN upper(trim(r.isCareHome)) = 'N' THEN FALSE
            ELSE NULL
        END AS is_care_home,
        TRY_CAST(r.careHomeBedCount AS INTEGER) AS care_home_bed_count,
        id_dir._id AS inspection_directorate_id,
        ic._id AS primary_inspection_category_id,
        r.address AS address,
        r.city AS city,
        pc._id AS postcode,
        r.parliamentaryConstituency AS parliamentary_constituency,
        TRY_CAST(r.lat AS DOUBLE) AS lat,
        TRY_CAST(r.long AS DOUBLE) AS long,
        CASE
            WHEN r.phone IS NULL OR trim(CAST(r.phone AS VARCHAR)) IN ('', '-') THEN NULL
            ELSE '0' || split_part(CAST(r.phone AS VARCHAR), '.', 1)
        END AS phone,
        NULLIF(trim(r.url), '') AS url,
        prv._id AS provider_id,
        br._id AS brand_id,
        CASE
            WHEN upper(trim(r.dualRegistered)) = 'Y' THEN TRUE
            WHEN upper(trim(r.dualRegistered)) = 'N' THEN FALSE
            ELSE NULL
        END AS dual_registered,
        NULLIF(trim(r.dualRegistrationPID), '') AS dual_registration_pid,
        TRY_CAST(r.hscaStartDate AS DATE) AS hsca_start_date
    FROM raw_dedup r
    LEFT JOIN org_types ot ON trim(r.type) = ot.name
    LEFT JOIN inspection_directorates id_dir ON trim(r.inspectionDirectorate) = id_dir.name
    LEFT JOIN inspection_categories ic ON trim(r.primaryInspectionCategory) = ic.name
    LEFT JOIN postcodes pc ON trim(r.postcode) = pc._id
    LEFT JOIN providers prv ON trim(r.providerId) = prv._id
    LEFT JOIN brands br ON trim(r.brandId) = br._id;
    """,

    # 6. Bridge Tables (Many-to-Many)
    """
    CREATE OR REPLACE TABLE location_service_types AS
    SELECT DISTINCT
        l._id AS location_id,
        st._id AS service_type_id
    FROM locations_raw r
    JOIN locations l ON trim(r._id) = l._id
    CROSS JOIN UNNEST(r.serviceTypes) AS t(serviceType)
    JOIN service_types st ON trim(t.serviceType) = st.name;
    """,

    """
    CREATE OR REPLACE TABLE location_service_user_bands AS
    SELECT DISTINCT
        l._id AS location_id,
        sub._id AS service_user_band_id
    FROM locations_raw r
    JOIN locations l ON trim(r._id) = l._id
    CROSS JOIN UNNEST(r.serviceUserBands) AS t(serviceUserBand)
    JOIN service_user_bands sub ON trim(t.serviceUserBand) = sub.name;
    """,

    """
    CREATE OR REPLACE TABLE location_regulated_activities AS
    SELECT DISTINCT
        l._id AS location_id,
        ra._id AS regulated_activity_id
    FROM locations_raw r
    JOIN locations l ON trim(r._id) = l._id
    CROSS JOIN UNNEST(r.regulatedActivities) AS t(regulatedActivity)
    JOIN regulated_activities ra ON trim(t.regulatedActivity) = ra.name;
    """,

    # 7. Time-Series / Fact Tables
    """
    CREATE OR REPLACE TABLE location_ratings AS
    SELECT
        cqc_uuid5(
            'location_ratings:' || l._id || ':' ||
            COALESCE(sg._id::VARCHAR, '') || ':' ||
            COALESCE(rt._id::VARCHAR, '') || ':' ||
            s.publicationDate::VARCHAR
        ) AS _id,
        l._id AS location_id,
        rt._id AS report_type_id,
        sg._id AS service_group_id,
        NULLIF(trim(s.ratingSafe), '') AS rating_safe,
        NULLIF(trim(s.ratingEffective), '') AS rating_effective,
        NULLIF(trim(s.ratingCaring), '') AS rating_caring,
        NULLIF(trim(s.ratingResponsive), '') AS rating_responsive,
        NULLIF(trim(s.ratingWellLed), '') AS rating_well_led,
        NULLIF(trim(s.ratingOverall), '') AS rating_overall,
        CASE
            WHEN upper(trim(s.inheritedRating)) = 'Y' THEN TRUE
            WHEN upper(trim(s.inheritedRating)) = 'N' THEN FALSE
            ELSE NULL
        END AS inherited_rating,
        s.publicationDate AS publication_date
    FROM location_ratings_staged s
    JOIN locations l ON trim(s.locationId) = l._id
    LEFT JOIN report_types rt ON trim(s.reportType) = rt.name
    LEFT JOIN service_groups sg ON trim(s.serviceGroup) = sg.name
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l._id, sg._id, rt._id, s.publicationDate
        ORDER BY s.publicationDate DESC
    ) = 1;
    """,

    """
    CREATE OR REPLACE TABLE provider_ratings AS
    SELECT
        cqc_uuid5(
            'provider_ratings:' || p._id || ':' ||
            COALESCE(sg._id::VARCHAR, '') || ':' ||
            COALESCE(rt._id::VARCHAR, '') || ':' ||
            s.publicationDate::VARCHAR
        ) AS _id,
        p._id AS provider_id,
        rt._id AS report_type_id,
        sg._id AS service_group_id,
        NULLIF(trim(s.ratingSafe), '') AS rating_safe,
        NULLIF(trim(s.ratingEffective), '') AS rating_effective,
        NULLIF(trim(s.ratingCaring), '') AS rating_caring,
        NULLIF(trim(s.ratingResponsive), '') AS rating_responsive,
        NULLIF(trim(s.ratingWellLed), '') AS rating_well_led,
        NULLIF(trim(s.ratingOverall), '') AS rating_overall,
        CASE
            WHEN upper(trim(s.inheritedRating)) = 'Y' THEN TRUE
            WHEN upper(trim(s.inheritedRating)) = 'N' THEN FALSE
            ELSE NULL
        END AS inherited_rating,
        s.publicationDate AS publication_date
    FROM provider_ratings_staged s
    JOIN providers p ON trim(s.providerId) = p._id
    LEFT JOIN report_types rt ON trim(s.reportType) = rt.name
    LEFT JOIN service_groups sg ON trim(s.serviceGroup) = sg.name
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p._id, sg._id, rt._id, s.publicationDate
        ORDER BY s.publicationDate DESC
    ) = 1;
    """,

    """
    CREATE OR REPLACE TABLE lad_populations AS
    SELECT
        cqc_uuid5('lad_populations:' || la._id || ':' || pop.year::VARCHAR) AS _id,
        la._id AS ladcd,
        CAST(pop.year AS INTEGER) AS year,
        CAST(pop.total AS INTEGER) AS total_population,
        CAST(pop."50-54" AS INTEGER) AS age_50_54,
        CAST(pop."55-59" AS INTEGER) AS age_55_59,
        CAST(pop."60-64" AS INTEGER) AS age_60_64,
        CAST(pop."65-69" AS INTEGER) AS age_65_69,
        CAST(pop."70-74" AS INTEGER) AS age_70_74,
        CAST(pop."75-79" AS INTEGER) AS age_75_79,
        CAST(pop."80-84" AS INTEGER) AS age_80_84,
        CAST(pop."85-89" AS INTEGER) AS age_85_89,
        CAST(pop."90+" AS INTEGER) AS age_90_plus
    FROM lad_pop_raw pop
    JOIN local_authorities la
        ON la._id = trim(pop.code)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY la._id, pop.year ORDER BY pop.total DESC) = 1;
    """,

    """
    CREATE OR REPLACE TABLE lad_life_expectancy AS
    SELECT
        cqc_uuid5('lad_life_expectancy:' || la._id || ':' || le.interval) AS _id,
        la._id AS ladcd,
        CAST(le.start_year AS INTEGER) AS start_year,
        trim(le.interval) AS interval,
        le."50-54" AS age_50_54,
        le."55-59" AS age_55_59,
        le."60-64" AS age_60_64,
        le."65-69" AS age_65_69,
        le."70-74" AS age_70_74,
        le."75-79" AS age_75_79,
        le."80-84" AS age_80_84,
        le."85-89" AS age_85_89,
        le."90+" AS age_90_plus
    FROM lad_life_exp_raw le
    JOIN local_authorities la
        ON la._id = trim(le.code)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY la._id, le.interval ORDER BY le.start_year DESC) = 1;
    """,
]


INITIALISE_RATINGS_QUERIES = [
    # Raw tables
    "CREATE OR REPLACE TABLE location_ratings_raw AS SELECT * FROM location_ratings_df",
    "CREATE OR REPLACE TABLE provider_ratings_raw AS SELECT * FROM provider_ratings_df",

    # Past location ratings table
    """
    CREATE OR REPLACE TABLE past_location_ratings AS
    WITH base AS (
        SELECT
            cqcId,
            serviceGroup,
            reportType,
            NULLIF(inheritedRating, '') AS inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM location_ratings_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cqcId, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    dedup AS (
        SELECT DISTINCT
            cqcId,
            serviceGroup,
            reportType,
            NULLIF(inheritedRating, '') AS inheritedRating,
            domain,
            latestRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM location_ratings_raw
    ),
    pivoted AS (
        SELECT *
        FROM dedup
        PIVOT (
            FIRST(latestRating)
            FOR domain IN (
                'Safe' AS ratingSafe,
                'Effective' AS ratingEffective,
                'Caring' AS ratingCaring,
                'Responsive' AS ratingResponsive,
                'Well-led' AS ratingWellLed,
                'Overall' AS ratingOverall
            )
        )
    )
    SELECT
        cqc_uuid5(
            'past_location_ratings:' || b.cqcId || ':' ||
            COALESCE(b.serviceGroup, '') || ':' ||
            COALESCE(b.reportType, '') || ':' ||
            COALESCE(b.publicationDate::VARCHAR, '')
        ) AS _id,
        b.cqcId AS locationId,
        b.reportType,
        b.serviceGroup,
        p.ratingSafe,
        p.ratingEffective,
        p.ratingCaring,
        p.ratingResponsive,
        p.ratingWellLed,
        p.ratingOverall,
        b.inheritedRating,
        b.publicationDate
    FROM base b
    LEFT JOIN pivoted p USING (cqcId, serviceGroup);
    """,

    # Past provider ratings table
    """
    CREATE OR REPLACE TABLE past_provider_ratings AS
    WITH base AS (
        SELECT
            cqcId,
            serviceGroup,
            reportType,
            NULLIF(inheritedRating, '') AS inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM provider_ratings_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cqcId, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    dedup AS (
        SELECT DISTINCT
            cqcId,
            serviceGroup,
            reportType,
            NULLIF(inheritedRating, '') AS inheritedRating,
            domain,
            latestRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM provider_ratings_raw
    ),
    pivoted AS (
        SELECT *
        FROM dedup
        PIVOT (
            FIRST(latestRating)
            FOR domain IN (
                'Safe' AS ratingSafe,
                'Effective' AS ratingEffective,
                'Caring' AS ratingCaring,
                'Responsive' AS ratingResponsive,
                'Well-led' AS ratingWellLed,
                'Overall' AS ratingOverall
            )
        )
    )
    SELECT
        cqc_uuid5(
            'past_provider_ratings:' || b.cqcId || ':' ||
            COALESCE(b.serviceGroup, '') || ':' ||
            COALESCE(b.reportType, '') || ':' ||
            COALESCE(b.publicationDate::VARCHAR, '')
        ) AS _id,
        b.cqcId AS providerId,
        b.reportType,
        b.serviceGroup,
        p.ratingSafe,
        p.ratingEffective,
        p.ratingCaring,
        p.ratingResponsive,
        p.ratingWellLed,
        p.ratingOverall,
        b.inheritedRating,
        b.publicationDate
    FROM base b
    LEFT JOIN pivoted p USING (cqcId, serviceGroup);
    """,
]
