"""Sync data with AuraDB."""

# Imports
import json
import asyncio
import datetime
from loguru import logger
from src.utils.connectors import AuraDB
from src.utils.queries import load_schema
from src.utils.ops import upsert_nodes, create_relationships, asyncify
from src.config import PROCESSED_DATA_DIR

# Create async versions of functions
async_upsert_nodes = asyncify(upsert_nodes)
async_create_relationships = asyncify(create_relationships)

class SyncManager:
    """Sync updated nodes with database."""
    async def __init__(self):
        self.driver = await AuraDB().connect_async()
        self.sync_time = datetime.datetime.now()
        self.schema = load_schema("aura_01")

    def load_data(self):
        """Load transformed data into a dict."""
        data = {}
        for filename, label in self.schema["filemap"].items():
            with open(PROCESSED_DATA_DIR / f"{filename}.json", "r", encoding="utf-8") as f:
                data[label] = json.load(f)

        return data

    async def fetch_last_sync_time(self):
        """Fetch the last sync time."""
        query = """
        MATCH (s:SyncTime)
        RETURN s.ts AS ts
        ORDER BY s.ts DESC LIMIT 1
        """
        async with self.driver.session() as session:
            result = await session.run(query)
            record = await result.single()
            if record:
                return datetime.datetime.fromisoformat(record["ts"])
            return datetime.datetime.min  # first sync

    async def write_sync_time(self):
        """Create SyncTime node"""
        query = """
        MERGE (new:SyncTime {ts: $ts})
        WITH new
        OPTIONAL MATCH (prev:SyncTime)
        WHERE prev.ts < $ts
        WITH new, prev ORDER BY prev.ts DESC LIMIT 1
        FOREACH (_ IN CASE WHEN prev IS NULL THEN [] ELSE [1] END |
            MERGE (prev)-[:NEXT]->(new)
        )
        """
        async with self.driver.session() as session:
            await session.run(query, ts=self.sync_time.isoformat())

    def filter_updated(self, docs, lst):
        """Filter docs by updated_at"""
        updated = []
        for d in docs:
            if "updated_at" not in d:
                continue
            doc_time = datetime.datetime.fromisoformat(d["updatedAt"])
            if doc_time > lst:
                updated.append(d)
        return updated

    async def run(self):
        """Main sync"""
        logger.info("Starting sync...")
        data = self.load_data()
        lst = await self.fetch_last_sync_time()
        logger.info(f"Last sync time: {lst}")

        # Filtered payloads
        filtered = {
            label: self.filter_updated(docs, lst)
            for label, docs in data.items()
        }

        # Log counts
        for label, docs in filtered.items():
            logger.info(f"{label}: {len(docs)} documents to upsert")

        # Upsert nodes
        async with self.driver.session() as session:
            await asyncio.gather(
                *[
                    session.execute_write(async_upsert_nodes, label, docs)
                    for label, docs in filtered.items()
                    if docs
                ]
            )

        # Create relationships
        rel_cfgs = self.schema["relationships"]
        async with self.driver.session() as session:
            await asyncio.gather(
                *[
                    session.execute_write(async_create_relationships, cfg, filtered)
                    for cfg in rel_cfgs
                    if filtered
                ]
            )

        # Write sync time node
        await self.write_sync_time()

        await self.driver.close()
        logger.info("Sync complete.")

# Run
async def main():
    """Run sync."""
    sync = SyncManager()
    await sync.run()

asyncio.run(main())
