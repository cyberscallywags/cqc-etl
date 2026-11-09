"""Neo4j AuraDB & Graph Projection utility functions"""

# Imports
import json
from pathlib import Path

from loguru import logger
import psycopg
from psycopg.rows import dict_row

from src.config import SCHEMAS_DIR
from src.utils.parsers import clean_documents


def clear_all_nodes(driver):
    """Clear all nodes in graph."""
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)
    logger.info("All nodes and relationships cleared.")


async def ensure_neo4j_constraints(driver, entities: list[dict]):
    """Ensure uniqueness constraints on _id for every entity label in Neo4j."""
    async with driver.session() as session:
        for entity in entities:
            label = entity["label"]
            constraint_name = f"uniq_{label.lower()}_id"
            query = (
                f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                f"FOR (n:{label}) REQUIRE n._id IS UNIQUE"
            )
            await session.run(query)
    logger.info(f"Verified Neo4j uniqueness constraints for {len(entities)} node labels.")


def fetch_entity_rows_from_pg(pg_conn: psycopg.Connection, entity_cfg: dict) -> list[dict]:
    """
    Fetch rows for a single entity table from PostgreSQL, aliasing snake_case columns
    to camelCase Neo4j properties as defined in kg_01.json.
    """
    table = entity_cfg["filename"]
    props = entity_cfg["properties"]

    select_parts = []
    for neo4j_prop, meta in props.items():
        pg_col = meta.get("column", neo4j_prop)
        prop_type = meta.get("type", "string")
        if prop_type in ("string", "date") and pg_col.endswith("_id") or pg_col == "_id":
            select_parts.append(f'"{pg_col}"::TEXT AS "{neo4j_prop}"')
        elif prop_type == "date":
            select_parts.append(f'"{pg_col}"::TEXT AS "{neo4j_prop}"')
        else:
            select_parts.append(f'"{pg_col}" AS "{neo4j_prop}"')

    query = f'SELECT {", ".join(select_parts)} FROM "{table}"'
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        raw_rows = cur.fetchall()

    return clean_documents(raw_rows)


def fetch_relationship_edges_from_pg(
    pg_conn: psycopg.Connection, rel_cfg: dict
) -> tuple[list[dict], list[dict]]:
    """
    Query PostgreSQL to produce edge lists for Neo4j projection.
    Returns (primary_edges, previous_chain_edges).
    """
    kind = rel_cfg.get("kind", "fk")

    if kind == "fk":
        source_table = rel_cfg["source"]
        source_col = rel_cfg["source_col"]
        query = f"""
            SELECT "_id"::TEXT AS source_id, "{source_col}"::TEXT AS target_id
            FROM "{source_table}"
            WHERE "{source_col}" IS NOT NULL
        """
        with pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall(), []

    if kind == "bridge":
        bridge_table = rel_cfg["bridge_table"]
        source_col = rel_cfg["source_col"]
        target_col = rel_cfg["target_col"]
        query = f"""
            SELECT "{source_col}"::TEXT AS source_id, "{target_col}"::TEXT AS target_id
            FROM "{bridge_table}"
        """
        with pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall(), []

    if kind == "time_series":
        target_table = rel_cfg["target_table"]
        target_fk_col = rel_cfg["target_fk_col"]
        timestamp_col = rel_cfg["timestamp_col"]
        disc_cols = rel_cfg.get("discriminator_cols", []) or []
        rel_prop_cols = rel_cfg.get("rel_prop_cols", {}) or {}

        partition_cols = ", ".join(f'"{c}"' for c in [target_fk_col, *disc_cols])

        prop_selects = ""
        if rel_prop_cols:
            prop_selects = ", " + ", ".join(
                f'"{pg_col}"::TEXT AS "{neo_prop}"'
                for neo_prop, pg_col in rel_prop_cols.items()
            )

        latest_query = f"""
            SELECT
                "{target_fk_col}"::TEXT AS source_id,
                "_id"::TEXT AS target_id
                {prop_selects}
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY "{timestamp_col}" DESC, "_id"
                    ) AS rn
                FROM "{target_table}"
                WHERE "{target_fk_col}" IS NOT NULL
            ) sub
            WHERE rn = 1
        """

        prev_query = f"""
            SELECT source_id, target_id
            FROM (
                SELECT
                    "_id"::TEXT AS source_id,
                    LEAD("_id"::TEXT) OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY "{timestamp_col}" DESC, "_id"
                    ) AS target_id
                FROM "{target_table}"
                WHERE "{target_fk_col}" IS NOT NULL
            ) sub
            WHERE target_id IS NOT NULL
        """

        with pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(latest_query)
            latest_edges = clean_documents(cur.fetchall())
            cur.execute(prev_query)
            prev_edges = cur.fetchall()
            return latest_edges, prev_edges

    raise ValueError(f"Unknown relationship kind: {kind}")


async def upsert_nodes(tx, entry, id_field: str = "_id", verbose: bool = False):
    """Generic AuraDB/Neo4j node upsert function."""
    label, rows = next(iter(entry.items()))

    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{_id: row.{id_field}}})
    SET n += row
    """
    await tx.run(query, rows=rows)
    if verbose:
        logger.success(f"Upserted {len(rows)} '{label}' nodes into Neo4j database.")


async def upsert_edge_batch(
    tx,
    source_label: str,
    target_label: str,
    rel_type: str,
    edges: list[dict],
    rel_prop_keys: list[str] | None = None,
):
    """Merge a batch of pre-computed (source_id, target_id) edges into Neo4j."""
    if not edges:
        return 0

    set_clause = ""
    if rel_prop_keys:
        props_map = ", ".join(f"{k}: edge.{k}" for k in rel_prop_keys)
        set_clause = f"SET r += {{{props_map}}}"

    query = f"""
    UNWIND $edges AS edge
    MATCH (s:{source_label} {{_id: edge.source_id}})
    MATCH (t:{target_label} {{_id: edge.target_id}})
    MERGE (s)-[r:{rel_type}]->(t)
    {set_clause}
    RETURN count(r) AS relationships_created
    """
    result = await tx.run(query, edges=edges)
    record = await result.single()
    return record["relationships_created"] if record else 0


async def create_relationships(tx, rel_cfg: dict, datasets: dict, id_batch=None):
    """
    Legacy Cypher relationship builder for JSON datasets.
    Preserved for compatibility.
    """
    (source_label, source_prop), = rel_cfg["from"].items()
    (target_label, target_prop), = rel_cfg["to"].items()
    rel = rel_cfg["type"]

    is_time_series = rel_cfg.get("is_time_series", False)
    timestamp_field = rel_cfg.get("timestamp")
    discriminators = rel_cfg.get("discriminators", []) or []
    rel_properties = rel_cfg.get("properties", {}) or {}

    source_docs = datasets[rel_cfg["source"]]
    updated_ids = id_batch or [i["_id"] for i in source_docs]

    if not updated_ids:
        return

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

    if not timestamp_field:
        raise ValueError(f"Time-series relationship '{rel}' requires 'timestamp' in config")

    disc_with = ", ".join([f"target.{d} AS {d}" for d in discriminators]) if discriminators else ""
    order_by_parts = [*discriminators, f"target.{timestamp_field} DESC"]
    order_by_clause = ", ".join(order_by_parts)
    group_with_parts = ["source", *discriminators, "collect(target) AS ts"]
    group_with_clause = ", ".join(group_with_parts)

    if rel_properties:
        rel_props_map = ", ".join(
            f"{prop}: latest.{prop}" for prop in rel_properties.keys()
        )
        rel_props_set = f"SET r += {{{rel_props_map}}}"
    else:
        rel_props_set = ""

    query = f"""
    MATCH (source:{source_label})
    WHERE source._id IN $ids
    OPTIONAL MATCH (target_to_clear:{target_label} {{{target_prop}: source.{source_prop}}})
    OPTIONAL MATCH (source)-[old_rel:{rel}]->(target_to_clear)
    OPTIONAL MATCH (target_to_clear)-[old_prev:PREVIOUS]->(:{target_label})
    DELETE old_rel, old_prev
    WITH DISTINCT source
    WHERE source.{source_prop} IS NOT NULL
    MATCH (target:{target_label} {{{target_prop}: source.{source_prop}}})
    {"WITH source, target, " + disc_with if disc_with else "WITH source, target"}
    ORDER BY {order_by_clause}
    WITH {group_with_clause}
    FOREACH (i IN range(0, size(ts)-2) |
      FOREACH (curr IN [ts[i]] |
        FOREACH (prev IN [ts[i+1]] |
          MERGE (curr)-[:PREVIOUS]->(prev)
        )
      )
    )
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


class OwlWriter:
    """Writes OWL from JSON knowledge graph with entities and relationships."""
    def __init__(self, base_uri="http://example.org/"):
        self.base = base_uri.rstrip("/") + "/"
        self.prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "ex": self.base
        }
        self.lines = []

    def write_prefixes(self):
        """Write OWL prefixes."""
        for pfx, uri in self.prefixes.items():
            self.lines.append(f"@prefix {pfx}: <{uri}> .")
        self.lines.append("")

    def add_class(self, class_name):
        """Add OWL class."""
        self.lines.append(f"ex:{class_name} a owl:Class .")

    def add_datatype_property(self, class_name, prop_name, prop_type):
        """Add OWL data type property."""
        xsd_type = self.map_type(prop_type)
        self.lines.append(
            f"ex:{prop_name} a owl:DatatypeProperty ;\n"
            f"    rdfs:domain ex:{class_name} ;\n"
            f"    rdfs:range xsd:{xsd_type} ."
        )

    def add_object_property(self, rel_name, domain, range_):
        """Add OWL object type property."""
        self.lines.append(
            f"ex:{rel_name} a owl:ObjectProperty ;\n"
            f"    rdfs:domain ex:{domain} ;\n"
            f"    rdfs:range ex:{range_} ."
        )

    def map_type(self, t):
        """Map types in JSON KG to OWL types."""
        mapping = {
            "string": "string",
            "float": "float",
            "int": "integer",
            "integer": "integer",
            "boolean": "boolean",
            "date": "dateTime"
        }
        return mapping.get(t, "string")

    def generate(self):
        """Generate OWL."""
        return "\n".join(self.lines)


def json_to_owl(kg_json: str, base_uri: str = "http://example.org/"):
    """Convert JSON knowledge graph to OWL."""
    writer = OwlWriter(base_uri)
    writer.write_prefixes()

    fp: Path = SCHEMAS_DIR / f"{kg_json}.json"
    with open(fp, "r", encoding="utf-8") as f:
        kg_schema = json.load(f)

    for entity in kg_schema["entities"]:
        label = entity["label"]
        writer.add_class(label)

        for prop, meta in entity["properties"].items():
            writer.add_datatype_property(label, prop, meta["type"])

    for rel in kg_schema["relationships"]:
        rel_name = rel["type"]
        domain = list(rel["from"].keys())[0]
        range_ = list(rel["to"].keys())[0]
        writer.add_object_property(rel_name, domain, range_)

    return writer.generate()
