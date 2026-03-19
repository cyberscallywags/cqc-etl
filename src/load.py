"""Load data into AuraDB"""

# Imports
import json
import asyncio
from loguru import logger
from rich.progress import (
    Progress, BarColumn, TimeElapsedColumn,
    TimeRemainingColumn, TextColumn,
    )
from src.utils.connectors import AuraDB
from src.utils.queries import load_schema
from src.utils.ops import upsert_nodes, create_relationships
from src.config import PROCESSED_DATA_DIR

# Load schema
schema: dict = load_schema("aura_01")
fmap: dict = schema["filemap"]
rel_cfgs: list[dict] = schema["relationships"]


def load_processed(filemap: dict):
    """Load all processed data into a dict."""
    data = {}
    for filename in filemap.keys():
        with open(PROCESSED_DATA_DIR / f"{filename}.json", "r", encoding="utf-8") as f:
            data[filename] = json.load(f)

    return data


async def worker(driver, sem, queue, progress, progress_tasks, global_task):
    """Worker that processes chunks and updates progress bars."""
    while True:
        label, chunk = await queue.get()
        try:
            async with sem:
                async with driver.session() as session:
                    await session.execute_write(upsert_nodes, {label: chunk})

            # Update per-label progress
            progress.update(progress_tasks[label], advance=1)

            # Update global progress
            progress.update(global_task, advance=1)

        finally:
            queue.task_done()


async def upsert_all_nodes(
    driver,
    filemap: dict,
    data: dict,
    sem,
    batch_size=1000,
    num_workers=10
):
    queue = asyncio.Queue()

    # Rich progress display
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
        # First pass: create tasks and compute totals
        for filename, label in filemap.items():
            rows = data.get(filename, [])
            if not rows:
                continue

            total_rows += len(rows)
            chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
            total_chunks += len(chunks)

            # Create per-label progress bar
            task_id = progress.add_task(
                f"[cyan]{label}",
                total=len(chunks),
                row_count=len(rows),
            )
            progress_tasks[label] = task_id

            # Enqueue chunks
            for chunk in chunks:
                queue.put_nowait((label, chunk))

        # Global summary bar
        global_task = progress.add_task(
            "[magenta]Total",
            total=total_chunks,
            row_count=total_rows,
        )

        # Start workers
        workers = [
            asyncio.create_task(worker(driver, sem, queue, progress, progress_tasks, global_task))
            for _ in range(num_workers)
        ]

        # Wait for all tasks to finish
        await queue.join()

        # Cancel workers
        for w in workers:
            w.cancel()

    logger.success("All labels upserted.")


async def relationship_with_session(driver, cfg, data, batch_size=2000):
    """Run relationship creation in batches of source IDs."""
    source_docs = data[cfg["source"]]
    all_ids = [i["_id"] for i in source_docs]
    id_batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]

    for id_batch in id_batches:
        async with driver.session() as session:
            await session.execute_write(create_relationships, cfg, data, id_batch)


async def main(max_concurrency=15):
    """Upsert nodes into AuraDB and create relationships."""
    data = load_processed(filemap=fmap)
    # Kill process if there's no data to upsert
    if not data:
        logger.error("No data available to upsert. Exiting...")
        return

    # Connect to database
    driver = await AuraDB().connect_async()
    sem = asyncio.Semaphore(max_concurrency)

    # Upsert nodes — each in its own session
    logger.info("Upserting nodes...")
    await upsert_all_nodes(driver, fmap, data, sem)

    # Prune configs using available data and create relationships
    pruned_cfgs = [i for i in rel_cfgs if i["source"] in data]
    await asyncio.gather(
        *[
            relationship_with_session(driver, cfg, data)
            for cfg in pruned_cfgs
        ]
    )

    await driver.close()


asyncio.run(main())
