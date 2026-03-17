"""DuckDB data transformation queries"""

# Imports
import json
from loguru import logger
from src.config import SCHEMAS_DIR
# pylint: disable=C0301


# FUNCTIONS

def run_queries(con, queries=None, select: str=""):
    """
    Run a list of DuckDB queries.
    Optionally choose from a predefined set of ETL pipeline queries.
    """
    query_map = {
        "": queries,
        "transform": TRANSFORM_QUERIES,
        "init_ratings": INITIALISE_RATINGS_QUERIES
        }

    if select not in query_map:
        raise ValueError(
            "Select parameter must either be 'transform', 'init_ratings' or left blank."
            )

    # Use the selected query list from the map
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
    # Raw tables
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

    # LOCATIONS AND PROVIDERS

    # Final locations table
    """
    CREATE OR REPLACE TABLE locations AS
    SELECT
        _id,
        odsCode,
        name,
        type,
        dormant,
        isCareHome,
        careHomeBedCount,
        inspectionDirectorate,
        primaryInspectionCategory,
        regulatedActivities,
        serviceTypes,
        serviceUserBands,
        address,
        city,
        postcode,
        parliamentaryConstituency,
        lat,
        long,
        CASE
            WHEN phone IS NULL THEN NULL
            ELSE '0' || split_part(CAST(phone AS VARCHAR), '.', 1)
        END AS phone,
        url,
        providerId,
        brandId,
        dualRegistered,
        dualRegistrationPID,
        TRY_CAST(hscaStartDate AS DATE) AS hscaStartDate
    FROM locations_raw;
    """,

    # Location ratings table
    """
    CREATE OR REPLACE TABLE location_ratings AS
    WITH base AS (
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
    pivoted AS (
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
            FOR domain
            IN (
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
        uuid() AS _id,
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
    FROM base b
    LEFT JOIN pivoted p USING (locationId, serviceGroup);
    """,

    """
    CREATE OR REPLACE TABLE location_ratings AS
    WITH combined AS (
        SELECT
            _id, locationId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM location_ratings

        UNION ALL

        SELECT
            _id, locationId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM past_location_ratings
    ),
    deduped AS (
        SELECT *
        FROM combined
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY locationId, serviceGroup, publicationDate, reportType
            ORDER BY publicationDate DESC
        ) = 1
    )
    SELECT * FROM deduped;
    """,

    # Final providers table
    """
    CREATE OR REPLACE TABLE providers AS
    SELECT DISTINCT
        providerId AS _id,
        providerName AS name,
        providerType AS type,
        providerInspectionDirectorate AS inspectionDirectorate,
        providerPrimaryInspectionCategory AS primaryInspectionCategory,
        provideraddress AS address,
        providercity AS city,
        providerPostcode AS postcode,
        providerParliamentaryConstituency AS parliamentaryConstituency,
        providerLat AS lat,
        providerLong AS long,
        CASE
            WHEN providerPhone IS NULL THEN NULL
            ELSE '0' || split_part(CAST(providerPhone AS VARCHAR), '.', 1)
        END AS phone,
        providerUrl AS url,
        TRY_CAST(providerHscaStartDate AS DATE) AS hscaStartDate
    FROM locations_raw;
    """,

    # Provider ratings table
    """
    CREATE OR REPLACE TABLE provider_ratings AS
    WITH base AS (
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
    pivoted AS (
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
            FOR domain
            IN (
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
        uuid() AS _id,
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
    FROM base b
    LEFT JOIN pivoted p USING (providerId, serviceGroup);
    """,

    """
    CREATE OR REPLACE TABLE provider_ratings AS
    WITH combined AS (
        SELECT
            _id, providerId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM provider_ratings

        UNION ALL

        SELECT
            _id, providerId, reportType, serviceGroup,
            ratingSafe, ratingEffective, ratingCaring,
            ratingResponsive, ratingWellLed, ratingOverall,
            inheritedRating, publicationDate
        FROM past_provider_ratings
    ),
    deduped AS (
        SELECT *
        FROM combined
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY providerId, serviceGroup, publicationDate, reportType
            ORDER BY publicationDate DESC
        ) = 1
    )
    SELECT * FROM deduped;
    """,


    # Services user bands table
    """
    CREATE OR REPLACE TABLE service_user_bands AS
    WITH distinct_subs AS (
        SELECT DISTINCT
            serviceUserBand
        FROM locations
        CROSS JOIN UNNEST(serviceUserBands) AS t(serviceUserBand)
        WHERE serviceUserBand IS NOT NULL
        AND serviceUserBand <> 'nan'
    ),
    ordered AS (
        SELECT
            serviceUserBand AS name,
            row_number() OVER (ORDER BY serviceUserBand) AS rn
        FROM distinct_subs
    )
    SELECT
        'sub' || lpad(rn::VARCHAR, 2, '0') AS _id,
        name
    FROM ordered
    ORDER BY name;
    """,

    # Service types table
    """
    CREATE OR REPLACE TABLE service_types AS
    WITH distinct_types AS (
        SELECT DISTINCT
            serviceType
        FROM locations
        CROSS JOIN UNNEST(serviceTypes) AS t(serviceType)
        WHERE serviceType IS NOT NULL
        AND serviceType <> 'nan'
    ),
    ordered AS (
        SELECT
            serviceType AS name,
            row_number() OVER (ORDER BY serviceType) AS rn
        FROM distinct_types
    )
    SELECT
        'st' || lpad(rn::VARCHAR, 2, '0') AS _id,
        name
    FROM ordered
    ORDER BY name;
    """,

    # Service groups table
    """
    CREATE OR REPLACE TABLE service_groups AS
    WITH service_groups_raw AS (
        SELECT serviceGroup FROM location_ratings WHERE serviceGroup IS NOT NULL
        UNION
        SELECT serviceGroup FROM provider_ratings WHERE serviceGroup IS NOT NULL
    )
    SELECT
        'sg' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY serviceGroup) AS VARCHAR), 3, '00') AS _id,
        serviceGroup AS name
    FROM (
        SELECT DISTINCT serviceGroup
        FROM service_groups_raw
    )
    ORDER BY name;
    """,

    # Organisation types table
    """
    CREATE OR REPLACE TABLE org_types AS
    WITH org_types_raw AS (
        SELECT type FROM locations WHERE type IS NOT NULL
        UNION
        SELECT type FROM providers WHERE type IS NOT NULL
    )
    SELECT
        'ot' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY type) AS VARCHAR), 2, '0') AS _id,
        type AS name
    FROM (
        SELECT DISTINCT type
        FROM org_types_raw
    )
    ORDER BY name;
    """,

    # Inspection categories table
    """
    CREATE OR REPLACE TABLE inspection_categories AS
    WITH inspection_categories_raw AS (
        SELECT primaryInspectionCategory FROM locations WHERE primaryInspectionCategory IS NOT NULL
        UNION
        SELECT primaryInspectionCategory FROM providers WHERE primaryInspectionCategory IS NOT NULL
    )
    SELECT
        'ic' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY primaryInspectionCategory) AS VARCHAR), 2, '0') AS _id,
        primaryInspectionCategory AS name
    FROM (
        SELECT DISTINCT primaryInspectionCategory
        FROM inspection_categories_raw
    )
    ORDER BY name;
    """,

    # Report types table
    """
    CREATE OR REPLACE TABLE report_types AS
    WITH report_types_raw AS (
        SELECT reportType FROM location_ratings WHERE reportType IS NOT NULL AND reportType <> ''
        UNION
        SELECT reportType FROM provider_ratings WHERE reportType IS NOT NULL AND reportType <> ''
    )
    SELECT
        'rt' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY reportType) AS VARCHAR), 2, '0') AS _id,
        reportType AS name
    FROM (
        SELECT DISTINCT reportType
        FROM report_types_raw
    )
    ORDER BY name;
    """,

    # Brands table
    """
    CREATE OR REPLACE TABLE brands AS
    WITH brands_clean AS (
        SELECT brandId, brandName
        FROM locations_raw
        WHERE brandId IS NOT NULL AND brandId <> '-'
    )
    SELECT DISTINCT
        brandId AS _id,
        regexp_replace(brandName, '^BRAND ', '') AS name
    FROM brands_clean
    ORDER BY _id;
    """,

    # Inspection directorate table
    """
    CREATE OR REPLACE TABLE inspection_directorates AS
    SELECT
        'ins' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY inspectionDirectorate) AS VARCHAR), 2, '0') AS _id,
        inspectionDirectorate AS name
    FROM (
        SELECT DISTINCT inspectionDirectorate
        FROM locations
    )
    ORDER BY name;
    """

    # GEOSPATIAL

    # Filtered ONSPD table
    """
    CREATE OR REPLACE TABLE onspd_filtered AS
    WITH postcodes_unique AS (
        SELECT postcode FROM locations WHERE postcode IS NOT NULL
        UNION
        SELECT postcode FROM providers WHERE postcode IS NOT NULL
    )
    SELECT o.*
    FROM onspd_raw o
    JOIN postcodes_unique u
        ON o.pcds = u.postcode;
    """,

    # Full ONSPD table
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

    # Postcodes table
    """
    CREATE OR REPLACE TABLE postcodes AS
    SELECT DISTINCT
        pcds AS _id,
        ladcd,
        lat,
        long
    FROM onspd;
    """,

    # Final local authorities table
    """
    CREATE OR REPLACE TABLE local_authorities AS
    SELECT DISTINCT
        ladcd AS _id,
        ladnm AS name,
        ctycd,
        rgncd,
        icbcd,
        nhsercd,
        lad_lat as lat,
        lad_long as long,
    FROM onspd
    WHERE ladcd IS NOT NULL;
    """,

    # Final counties table
    """
    CREATE OR REPLACE TABLE counties AS
    SELECT DISTINCT
        ctycd AS _id,
        ctynm AS name,
        rgncd
    FROM onspd
    WHERE ctycd IS NOT NULL;
    """,

    # Final ICBs table
    """
    CREATE OR REPLACE TABLE icbs AS
    SELECT DISTINCT
        icbcd AS _id,
        icbnm AS name
    FROM onspd
    WHERE icbcd IS NOT NULL;
    """,

    # Final regions table
    """
    CREATE OR REPLACE TABLE regions AS
    SELECT DISTINCT
        rgncd AS _id,
        rgnnm AS name
    FROM onspd
    WHERE rgncd IS NOT NULL;
    """,

    # Final NHSER table
    """
    CREATE OR REPLACE TABLE nhser AS
    SELECT DISTINCT
        nhsercd AS _id,
        nhsernm AS name
    FROM onspd
    WHERE nhsercd IS NOT NULL;
    """,

    # Local authority by population
    """
    CREATE OR REPLACE TABLE lad_populations AS
    SELECT
        uuid() AS _id,
        la._id AS ladcd,
        pop.YEAR AS year,
        pop.TOTAL AS totalPopulation,
        pop."50-54",
        pop."55-59",
        pop."60-64",
        pop."65-69",
        pop."70-74",
        pop."75-79",
        pop."80-84",
        pop."85-89",
        pop."90+"
    FROM lad_pop_raw pop
    JOIN local_authorities la
        ON la._id = pop.code;
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
        uuid() AS _id,
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
        uuid() AS _id,
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
