import json
import asyncio
from datetime import datetime
from loguru import logger
from src.utils.connectors import AuraDB
from src.utils.ops import upsert_nodes, create_relationships, get_relationships
from src.config import PROCESSED_DATA_DIR


class SyncManager:
    def __init__(self):
        self.db = AuraDB()
        self.driver = self.db.connect_async()
        self.sync_time = datetime.now()  # new sync timestamp

        self.node_sources = {
            "Region": "regions",
            "LocalAuthority": "local_authorities",
            "Postcode": "postcodes",
            "Location": "locations",
            "Provider": "providers",
            "ServiceType": "service_types",
            "Service": "services",
        }

    # -----------------------------
    # Load JSON
    # -----------------------------
    def load_data(self):
        data = {}
        for label, filename in self.node_sources.items():
            with open(PROCESSED_DATA_DIR / f"{filename}.json", "r", encoding="utf-8") as f:
                data[label] = json.load(f)
        return data

    # -----------------------------
    # Fetch last sync time
    # -----------------------------
    async def get_last_sync_time(self):
        query = """
        MATCH (s:SyncTime)
        RETURN s.ts AS ts
        ORDER BY s.ts DESC LIMIT 1
        """
        async with self.driver.session() as session:
            result = await session.run(query)
            record = await result.single()
            if record:
                return datetime.fromisoformat(record["ts"])
            return datetime.min  # first sync

    # -----------------------------
    # Filter docs by updated_at
    # -----------------------------
    def filter_updated(self, docs, last_sync):
        updated = []
        for d in docs:
            if "updated_at" not in d:
                continue
            doc_time = datetime.fromisoformat(d["updated_at"])
            if doc_time > last_sync:
                updated.append(d)
        return updated

    # -----------------------------
    # Create SyncTime node
    # -----------------------------
    async def write_sync_time(self):
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

    # -----------------------------
    # Main sync
    # -----------------------------
    async def run(self):
        logger.info("Starting sync...")
        data = self.load_data()
        last_sync = await self.get_last_sync_time()
        logger.info(f"Last sync time: {last_sync}")

        # Filtered payloads
        filtered = {
            label: self.filter_updated(docs, last_sync)
            for label, docs in data.items()
        }

        # Log counts
        for label, docs in filtered.items():
            logger.info(f"{label}: {len(docs)} documents to upsert")

        # Upsert nodes
        async with self.driver.session() as session:
            await asyncio.gather(
                *[
                    session.execute_write(upsert_nodes, label, docs)
                    for label, docs in filtered.items()
                    if docs
                ]
            )

        # Create relationships
        rel_maps = get_relationships(data)
        async with self.driver.session() as session:
            await asyncio.gather(
                *[
                    session.execute_write(create_relationships, rel_map, rel, source_docs)
                    for rel_map, rel, source_docs in rel_maps
                    if source_docs
                ]
            )

        # Write sync time node
        await self.write_sync_time()

        await self.driver.close()
        logger.info("Sync complete.")


# -----------------------------
# Run
# -----------------------------
async def main():
    sync = SyncManager()
    await sync.run()

asyncio.run(main())
