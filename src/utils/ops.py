"""Neo4j AuraDB utlity functions"""

# Imports
from loguru import logger

def upsert_nodes(tx, label, rows, id_field="_id"):
    """Generic AuraDB upsert function"""
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{_id: row.{id_field}}})
    SET n += row
    """
    tx.run(query, rows=rows)
    logger.success(f"Upserted {len(rows)} '{label}' nodes into Neo4j database.")


def clear_all_nodes(driver):
    """Clear all nodes in graph."""
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)
    logger.info("All nodes and relationships cleared.")


def create_relationships(tx, rel_map, rel: str, source_docs):
    """
    Create relationship between two labels
    while pruning old relationships for the updated batch.
    """
    # Extract node labels, fields, updated source _ids
    source_label = rel_map["labels"][0]
    target_label = rel_map["labels"][1]
    source_prop = rel_map["props"][0]
    target_prop = rel_map["props"][1]
    updated_ids = [i["_id"] for i in source_docs]

    if not updated_ids:
        return

    # Delete only the specific relationship type for nodes being updated
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

    result = tx.run(query, ids=updated_ids)
    count = result.single()["relationships_created"]
    logger.info(f"Created {count} relationships of type {rel}")

def get_relationships(data):
    """Return relationship mapping."""

    rel_maps = [
        ({"labels": ["Location", "Provider"], "props": ["provider_id", "id"]},
         "HAS_PROVIDER", data["locations"]),
        ({"labels": ["Location", "Postcode"], "props": ["postcode", "postcode"]},
         "IN_POSTCODE", data["locations"]),
        ({"labels": ["Location", "ServiceType"], "props": ["service_types", "name"]},
         "HAS_SERVICE_TYPE", data["locations"]),
        ({"labels": ["Location", "Service"], "props": ["services", "name"]},
         "HAS_SERVICE", data["locations"]),
        ({"labels": ["Postcode", "LocalAuthority"], "props": ["local_authority", "name"]},
         "IN_LA", data["postcodes"]),
        ({"labels": ["LocalAuthority", "Region"], "props": ["region", "name"]},
         "IN_REGION", data["local_authorities"]),
    ]

    return rel_maps
