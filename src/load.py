"""Project PostgreSQL Source of Truth into Neo4j Graph Database."""

# Imports
import asyncio
import time

from loguru import logger
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.utils.connectors import AuraDB, PostgresDB
from src.utils.ops import (
    ensure_neo4j_constraints,
    fetch_entity_rows_from_pg,
    fetch_relationship_edges_from_pg,
    upsert_edge_batch,
    upsert_nodes,
)
from src.utils.queries import load_schema


def load_entities_from_postgres(entities: list[dict]) -> dict[str, list[dict]]:
    """Fetch all entity tables from PostgreSQL mapped to Neo4j camelCase properties."""
    data: dict[str, list[dict]] = {}
    pg_db = PostgresDB()
    with pg_db.connect() as pg_conn:
        for entity in entities:
            filename = entity["filename"]
            rows = fetch_entity_rows_from_pg(pg_conn, entity)
            data[filename] = rows
            logger.info(f"Loaded {len(rows):,} rows from PostgreSQL table '{filename}'.")
    return data


async def node_worker(driver, sem, queue, progress, progress_tasks, global_task):
    """Worker that upserts node chunks and updates progress bars."""
    while True:
        label, chunk = await queue.get()
        try:
            async with sem:
                async with driver.session() as session:
                    await session.execute_write(upsert_nodes, {label: chunk})

            progress.update(progress_tasks[label], advance=1)
            progress.update(global_task, advance=1)
        finally:
            queue.task_done()


async def upsert_all_nodes(
    driver,
    filemap: dict[str, str],
    data: dict[str, list[dict]],
    sem: asyncio.Semaphore,
    batch_size: int = 1000,
    num_workers: int = 10,
):
    """Upsert all entity nodes into Neo4j with Rich progress display."""
    queue: asyncio.Queue = asyncio.Queue()

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} chunks"),
        TextColumn("• {task.fields[row_count]} rows"),
        TextColumn("• {task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )

    progress_tasks = {}
    total_chunks = 0
    total_rows = 0

    with progress:
        for filename, label in filemap.items():
            rows = data.get(filename, [])
            if not rows:
                continue

            total_rows += len(rows)
            chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
            total_chunks += len(chunks)

            task_id = progress.add_task(
                f"[cyan]{label}",
                total=len(chunks),
                row_count=len(rows),
            )
            progress_tasks[label] = task_id

            for chunk in chunks:
                queue.put_nowait((label, chunk))

        global_task = progress.add_task(
            "[magenta]Total Nodes",
            total=total_chunks,
            row_count=total_rows,
        )

        workers = [
            asyncio.create_task(
                node_worker(driver, sem, queue, progress, progress_tasks, global_task)
            )
            for _ in range(num_workers)
        ]

        await queue.join()

        for w in workers:
            w.cancel()

    logger.success("All node labels projected into Neo4j.")


async def project_relationship_cfg(
    driver,
    sem: asyncio.Semaphore,
    rel_cfg: dict,
    primary_edges: list[dict],
    prev_edges: list[dict],
    batch_size: int = 2000,
):
    """Project primary edges and optional [:PREVIOUS] time-series chain edges into Neo4j."""
    (source_label, _), = rel_cfg["from"].items()
    (target_label, _), = rel_cfg["to"].items()
    rel_type = rel_cfg["type"]
    rel_prop_keys = list((rel_cfg.get("rel_prop_cols") or {}).keys())

    # Clear existing relationships for this pattern before re-projecting
    async with sem:
        async with driver.session() as session:
            await session.run(
                f"MATCH (:{source_label})-[r:{rel_type}]->(:{target_label}) DELETE r"
            )
            if rel_cfg.get("is_time_series"):
                await session.run(
                    f"MATCH (:{target_label})-[p:PREVIOUS]->(:{target_label}) DELETE p"
                )

    total_created = 0
    for i in range(0, len(primary_edges), batch_size):
        batch = primary_edges[i:i + batch_size]
        async with sem:
            async with driver.session() as session:
                created = await session.execute_write(
                    upsert_edge_batch,
                    source_label,
                    target_label,
                    rel_type,
                    batch,
                    rel_prop_keys,
                )
                total_created += created

    logger.info(
        f"Projected {total_created:,} (:{source_label})-[:{rel_type}]->(:{target_label}) relationships."
    )

    if prev_edges:
        total_prev = 0
        for i in range(0, len(prev_edges), batch_size):
            batch = prev_edges[i:i + batch_size]
            async with sem:
                async with driver.session() as session:
                    created = await session.execute_write(
                        upsert_edge_batch,
                        target_label,
                        target_label,
                        "PREVIOUS",
                        batch,
                        None,
                    )
                    total_prev += created
        logger.info(
            f"Projected {total_prev:,} (:{target_label})-[:PREVIOUS]->(:{target_label}) time-series links."
        )


async def main(max_concurrency: int = 15):
    """Project PostgreSQL Source of Truth into Neo4j."""
    start = time.perf_counter()
    kg: dict = load_schema("kg_01")
    entities: list[dict] = kg["entities"]
    fmap: dict[str, str] = {i["filename"]: i["label"] for i in entities}
    rel_cfgs: list[dict] = kg["relationships"]

    logger.info("Reading entity tables from PostgreSQL Source of Truth...")
    data = load_entities_from_postgres(entities)
    if not data:
        logger.error("No data available in PostgreSQL to project. Exiting...")
        return

    driver = await AuraDB().connect_async()
    sem = asyncio.Semaphore(max_concurrency)

    await ensure_neo4j_constraints(driver, entities)

    logger.info("Projecting nodes into Neo4j...")
    await upsert_all_nodes(driver, fmap, data, sem)

    logger.info("Computing relationship edge sets from PostgreSQL...")
    pg_db = PostgresDB()
    edge_Work = []
    with pg_db.connect() as pg_conn:
        for cfg in rel_cfgs:
            primary_edges, prev_edges = fetch_relationship_edges_from_pg(pg_conn, cfg)
            edge_Work.append((cfg, primary_edges, prev_edges))

    logger.info("Projecting relationships into Neo4j...")
    await asyncio.gather(
        *[
            project_relationship_cfg(driver, sem, cfg, primary_edges, prev_edges)
            for cfg, primary_edges, prev_edges in edge_Work
        ]
    )

    await driver.close()
    elapsed = time.perf_counter() - start
    logger.success(f"Neo4j graph projection complete in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())
