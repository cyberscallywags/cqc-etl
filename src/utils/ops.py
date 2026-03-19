"""Neo4j AuraDB utlity functions"""

# Imports
from loguru import logger


def clear_all_nodes(driver):
    """Clear all nodes in graph."""
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)
    logger.info("All nodes and relationships cleared.")


async def upsert_nodes(tx, entry, id_field: str="_id", verbose: bool=False):
    """Generic AuraDB upsert function"""
    # Extract label and data
    label, rows = next(iter(entry.items()))

    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{_id: row.{id_field}}})
    SET n += row
    """
    await tx.run(query, rows=rows)
    if verbose:
        logger.success(f"Upserted {len(rows)} '{label}' nodes into Neo4j database.")


async def create_relationships(tx, rel_cfg: dict, datasets: dict, id_batch=None):
    """
    Create relationships between two labels.
    - For normal relationships: simple fan-out from source to target.
    - For time-series relationships: build chains per discriminator group,
      attach most recent to source, and chain older via [:PREVIOUS].
    """

    # Extract node labels, fields, relationship type
    (source_label, source_prop), = rel_cfg["from"].items()
    (target_label, target_prop), = rel_cfg["to"].items()
    rel = rel_cfg["type"]

    is_time_series = rel_cfg.get("is_time_series", False)
    timestamp_field = rel_cfg.get("timestamp")
    discriminators = rel_cfg.get("discriminators", []) or []
    rel_properties = rel_cfg.get("properties", {}) or {}

    # Extract updated source _ids
    source_docs = datasets[rel_cfg["source"]]
    updated_ids = id_batch or [i["_id"] for i in source_docs]

    if not updated_ids:
        return

    # Non–time-series
    if not is_time_series:
        query = f"""
        MATCH (source:{source_label})
        WHERE source._id IN $ids
        OPTIONAL MATCH (source)-[old_rel:{rel}]->()
        DELETE old_rel
        WITH source
        WHERE source.{source_prop} IS NOT NULL
        UNWIND source.{source_prop} AS value
        MATCH (target:{target_label} {{{target_prop}: value}})
        MERGE (source)-[:{rel}]->(target)
        RETURN count(*) AS relationships_created
        """
        result = await tx.run(query, ids=updated_ids)
        count = (await result.single())["relationships_created"]
        logger.info(f"Created {count} relationships of type {rel}")
        return

    # Time-series relationships
    if not timestamp_field:
        raise ValueError(f"Time-series relationship '{rel}' requires 'timestamp' in config")

    # Build dynamic Cypher fragments for discriminators and properties
    # e.g. "target.reportType AS reportType, target.serviceGroup AS serviceGroup"
    disc_with = ", ".join([f"target.{d} AS {d}" for d in discriminators]) if discriminators else ""
    # e.g. "reportType, serviceGroup, target.publicationDate DESC"
    order_by_parts = []
    order_by_parts.extend(discriminators)
    order_by_parts.append(f"target.{timestamp_field} DESC")
    order_by_clause = ", ".join(order_by_parts)

    # e.g. "source, reportType, serviceGroup, collect(target) AS ts"
    group_with_parts = ["source"]
    group_with_parts.extend(discriminators)
    group_with_parts.append("collect(target) AS ts")
    group_with_clause = ", ".join(group_with_parts)

    # Relationship properties copied from latest target node
    # e.g. "r += {date: latest.date, overallRating: latest.overallRating}"
    if rel_properties:
        rel_props_map = ", ".join(
            f"{prop}: latest.{prop}" for prop in rel_properties.keys()
        )
        rel_props_set = f"SET r += {{{rel_props_map}}}"
    else:
        rel_props_set = ""

    # Full time-series query:
    query = f"""
    // 1. Match updated sources
    MATCH (source:{source_label})
    WHERE source._id IN $ids

    // 2. Delete existing relationships of this type and PREVIOUS chains
    //    for all targets associated with this source
    OPTIONAL MATCH (target_to_clear:{target_label} {{{target_prop}: source.{source_prop}}})
    OPTIONAL MATCH (source)-[old_rel:{rel}]->(target_to_clear)
    OPTIONAL MATCH (target_to_clear)-[old_prev:PREVIOUS]->(:{target_label})
    DELETE old_rel, old_prev

    // 3. Re-match all relevant targets for this source
    WITH DISTINCT source
    WHERE source.{source_prop} IS NOT NULL
    MATCH (target:{target_label} {{{target_prop}: source.{source_prop}}})
    {"WITH source, target, " + disc_with if disc_with else "WITH source, target"}
    ORDER BY {order_by_clause}

    // 4. Group by discriminator combination and collect ordered targets
    WITH {group_with_clause}

    // 5. Create PREVIOUS chains within each discriminator group
    FOREACH (i IN range(0, size(ts)-2) |
      FOREACH (curr IN [ts[i]] |
        FOREACH (prev IN [ts[i+1]] |
          MERGE (curr)-[:PREVIOUS]->(prev)
        )
      )
    )

    // 6. Attach source -> most recent (head of ts) with rel,
    //    and set configured relationship properties from latest node
    WITH source, ts
    WHERE size(ts) > 0
    WITH source, head(ts) AS latest
    MERGE (source)-[r:{rel}]->(latest)
    {rel_props_set}
    RETURN count(DISTINCT r) AS relationships_created
    """

    result = await tx.run(query, ids=updated_ids)
    count = (await result.single())["relationships_created"]
    logger.info(f"Created {count} time-series relationships of type {rel}")
