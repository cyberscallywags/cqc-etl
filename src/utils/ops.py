"""Neo4j AuraDB utlity functions"""

# Imports
import json
from pathlib import Path
from loguru import logger
from src.config import SCHEMAS_DIR


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


async def create_relationships_simple(tx, cfg, id_batch):
    """
    Create simple fan-out relationships between two labels (from source to target).
    """
    (source_label, source_prop), = cfg["from"].items()
    (target_label, target_prop), = cfg["to"].items()
    rel = cfg["type"]

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

    result = await tx.run(query, ids=id_batch)
    count = (await result.single())["relationships_created"]
    logger.info(f"Created {count} simple relationships of type {rel}")


async def create_relationships_ts(tx, cfg, id_batch):
    """
    Create time series relationships between two labels by
    building chains per discriminator group: attach most recent
    to source, and chain older via [:PREVIOUS].
    """
    (source_label, source_prop), = cfg["from"].items()
    (target_label, target_prop), = cfg["to"].items()
    rel = cfg["type"]

    timestamp_field = cfg["timestamp"]
    discriminators = cfg.get("discriminators", []) or []
    rel_properties = cfg.get("properties", {}) or {}

    # Build dynamic fragments
    disc_with = ", ".join([f"target.{d} AS {d}" for d in discriminators])
    order_by = ", ".join([*discriminators, f"target.{timestamp_field} DESC"])
    group_with = ", ".join(["source", *discriminators, "collect(target) AS ts"])

    if rel_properties:
        props = ", ".join(f"{p}: latest.{p}" for p in rel_properties)
        rel_props_set = f"SET r += {{{props}}}"
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
        ORDER BY {order_by}

        WITH {group_with}
        WHERE size(ts) > 0

        CALL apoc.nodes.link(ts, 'PREVIOUS') YIELD nodes

        WITH source, head(ts) AS latest
        MERGE (source)-[r:{rel}]->(latest)
        {rel_props_set}

        RETURN count(DISTINCT r) AS relationships_created
    """

    result = await tx.run(query, ids=id_batch)
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
        self.lines.append("")  # blank line

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
            "date": "dateTime"
        }
        return mapping.get(t, "string")

    def generate(self):
        """Generate OWL."""
        return "\n".join(self.lines)


def json_to_owl(kg_json: str, base_uri: str="http://example.org/"):
    """Convert JSON knowledge graph to OWL."""
    writer = OwlWriter(base_uri)
    writer.write_prefixes()

    # Load JSON
    fp: Path = SCHEMAS_DIR / f"{kg_json}.json"
    with open(fp, "r", encoding="utf-8") as f:
        kg_schema = json.load(f)

    # Entities -> Classes + datatype properties
    for entity in kg_schema["entities"]:
        label = entity["label"]
        writer.add_class(label)

        for prop, meta in entity["properties"].items():
            writer.add_datatype_property(label, prop, meta["type"])

    # Relationships -> Object properties
    for rel in kg_schema["relationships"]:
        rel_name = rel["type"]
        domain = list(rel["from"].keys())[0]
        range_ = list(rel["to"].keys())[0]
        writer.add_object_property(rel_name, domain, range_)

    return writer.generate()
