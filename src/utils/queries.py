"""DuckDB data transformation queries"""

# pylint: disable=C0301

QUERIES = [
    # Raw tables
    "CREATE OR REPLACE TABLE onspd_raw AS SELECT * FROM onspd_df",
    "CREATE OR REPLACE TABLE la_districts_raw AS SELECT * FROM la_districts_df",
    "CREATE OR REPLACE TABLE counties_raw AS SELECT * FROM counties_df",
    "CREATE OR REPLACE TABLE icbs_raw AS SELECT * FROM icbs_df",
    "CREATE OR REPLACE TABLE regions_raw AS SELECT * FROM regions_df",
    "CREATE OR REPLACE TABLE nhser_raw AS SELECT * FROM nhser_df",
    "CREATE OR REPLACE TABLE directory_raw AS SELECT * FROM directory_df",
    "CREATE OR REPLACE TABLE locations_raw AS SELECT * FROM locations_df",
    "CREATE OR REPLACE TABLE providers_raw AS SELECT * FROM providers_df",
    "CREATE OR REPLACE TABLE lad_pop_raw AS SELECT * FROM lad_pop_df",

    # LOCATIONS AND PROVIDERS

    # Cleaned directory table
    """
    CREATE OR REPLACE TABLE directory AS
    SELECT
        _id,
        name,
        address,
        postcode,
        CASE
            WHEN phoneNo IS NULL THEN NULL
            ELSE '0' || split_part(CAST(phoneNo AS VARCHAR), '.', 1)
        END AS phoneNo,
        COALESCE(url, '') AS url,
        string_split(services, '|') AS services,
        string_split(serviceTypes, '|') AS serviceTypes,
        TRY_CAST(latestCheckDate AS DATE) AS latestCheckDate,
        providerId,
        providerName
    FROM directory_raw;
    """,

    # Cleaned locations table
    """
    CREATE OR REPLACE TABLE locations_cleaned AS
    SELECT
        _id, odsCode, name, type, isCareHome, primaryInspectionCategory,
        address, postcode, providerId, providerName, brandId,
        TRY_CAST(publicationDate AS DATE) AS latestCheckDate
    FROM locations_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _id
        ORDER BY TRY_CAST(publicationDate AS DATE) DESC
    ) = 1;
    """,

    # Final locations table
    """
    CREATE OR REPLACE TABLE locations AS
    WITH merged AS (
        SELECT
            COALESCE(l._id, d._id) AS _id,

            -- Dates used for conflict resolution
            l.latestCheckDate AS latestCheckDate_l,
            d.latestCheckDate AS latestCheckDate_d,
            GREATEST(l.latestCheckDate, d.latestCheckDate) AS latestCheckDate,

            -- Fields to resolve
            l.name AS name_l,
            d.name AS name_d,
            l.address AS address_l,
            d.address AS address_d,
            l.postcode AS postcode_l,
            d.postcode AS postcode_d,
            l.providerId AS providerId_l,
            d.providerId AS providerId_d,
            l.providerName AS providerName_l,
            d.providerName AS providerName_d,

            -- Keep all other fields
            l.odsCode,
            l.type,
            l.isCareHome,
            l.primaryInspectionCategory,
            l.brandId,
            d.phoneNo,
            d.url,
            d.services,
            d.serviceTypes,
        FROM locations_cleaned l
        FULL OUTER JOIN directory d USING (_id)
    ),
    resolved AS (
        SELECT
            _id,
            odsCode,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN name_l ELSE name_d END AS name,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN address_l ELSE address_d END AS address,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN postcode_l ELSE postcode_d END AS postcode,
            phoneNo,
            url,
            type,
            isCareHome,
            primaryInspectionCategory,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN providerId_l ELSE providerId_d END AS providerId,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN providerName_l ELSE providerName_d END AS providerName,
            brandId,
            services,
            serviceTypes,
            'http://www.cqc.org.uk/location/' || _id AS cqcUrl,
            latestCheckDate
        FROM merged
    )
    SELECT * FROM resolved;
    """,

    # Location ratings table
    """
    CREATE OR REPLACE TABLE location_ratings AS
    WITH base AS (
        SELECT
            _id AS locationId,
            serviceGroup,
            reportType,
            inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM locations_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY _id, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    pivoted AS (
        SELECT *
        FROM (
            SELECT
                _id AS locationId,
                serviceGroup,
                domain,
                latestRating
            FROM locations_raw
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

    # Cleaned providers table
    """
    CREATE OR REPLACE TABLE providers_cleaned AS
    SELECT
        _id, odsCode, name, type, primaryInspectionCategory,
        address, postcode, brandId,
        TRY_CAST(publicationDate AS DATE) AS latestCheckDate
    FROM providers_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _id
        ORDER BY TRY_CAST(publicationDate AS DATE) DESC
    ) = 1;
    """,

    # Extract provider IDs/names from directory
    """
    CREATE OR REPLACE TABLE directory_providers AS
    SELECT
        providerId AS _id,
        providerName AS name,
        TRY_CAST(latestCheckDate AS DATE) AS latestCheckDate
    FROM directory_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _id
        ORDER BY TRY_CAST(latestCheckDate AS DATE) DESC
    ) = 1;
    """,

    # Final providers table
    """
    CREATE OR REPLACE TABLE providers AS
    WITH merged AS (
        SELECT
            COALESCE(p._id, d._id) AS _id,

            -- Dates used for conflict resolution
            p.latestCheckDate AS latestCheckDate_l,
            d.latestCheckDate AS latestCheckDate_d,
            GREATEST(p.latestCheckDate, d.latestCheckDate) AS latestCheckDate,

            -- Fields to resolve
            p.name AS name_p,
            d.name AS name_d,

            -- Keep all other fields
            p.odsCode,
            p.type,
            p.primaryInspectionCategory,
            p.address,
            p.postcode,
            p.brandId
        FROM providers_cleaned p
        FULL OUTER JOIN directory_providers d USING (_id)
    ),
    resolved AS (
        SELECT
            _id,
            odsCode,
            CASE WHEN latestCheckDate_l >= latestCheckDate_d THEN name_p ELSE name_d END AS name,
            address,
            postcode,
            type,
            primaryInspectionCategory,
            brandId,
            'http://www.cqc.org.uk/provider/' || _id AS cqcUrl,
            latestCheckDate
        FROM merged
    )
    SELECT * FROM resolved;
    """,

    # Provider ratings table
    """
    CREATE OR REPLACE TABLE provider_ratings AS
    WITH base AS (
        SELECT
            _id AS providerId,
            serviceGroup,
            reportType,
            inheritedRating,
            TRY_CAST(publicationDate AS DATE) AS publicationDate
        FROM providers_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY _id, serviceGroup
            ORDER BY TRY_CAST(publicationDate AS DATE) DESC
        ) = 1
    ),
    pivoted AS (
        SELECT *
        FROM (
            SELECT
                _id AS providerId,
                serviceGroup,
                domain,
                latestRating
            FROM providers_raw
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

    # Services table
    """
    CREATE OR REPLACE TABLE services AS
    WITH distinct_services AS (
        SELECT DISTINCT
            service
        FROM locations
        CROSS JOIN UNNEST(services) AS t(service)
        WHERE service IS NOT NULL
        AND service <> 'nan'
    ),
    ordered AS (
        SELECT
            service AS name,
            row_number() OVER (ORDER BY service) AS rn
        FROM distinct_services
    )
    SELECT
        's' || lpad(rn::VARCHAR, 2, '0') AS _id,
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
        SELECT reportType FROM location_ratings WHERE reportType IS NOT NULL
        UNION
        SELECT reportType FROM provider_ratings WHERE reportType IS NOT NULL
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
    WITH brands_unique AS (
        SELECT brandId, brandName
        FROM locations_raw
        WHERE brandId IS NOT NULL AND brandId <> '-'
        
        UNION
        
        SELECT brandId, brandName
        FROM providers_raw
        WHERE brandId IS NOT NULL AND brandId <> '-'
    )
    SELECT
        brandId AS _id,
        regexp_replace(brandName, '^BRAND ', '') AS name
    FROM brands_unique
    ORDER BY _id;
    """,

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
