"""Transform CQC directory data"""

# Imports
from datetime import datetime
import pandas as pd
from loguru import logger
from src.utils.parsers import decompose_list
from src.utils.transforms import combine_csvs, make_documents
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

# Create timestamp
timestamp = datetime.now()

# Create and clean dataframe
df = combine_csvs(RAW_DATA_DIR / "directory")
df.columns = [
    "name", "aka", "address", "postcode", "phone_no",
    "url", "service_types", "latest_check_date", "services",
    "provider", "local_authority", "region",
    "location_url", "location_id", "provider_id"
    ]
df["latest_check_date"] = pd.to_datetime(df["latest_check_date"])
df["phone_no"] = df["phone_no"].astype(str).apply(lambda x:""if x=="nan" else f"0{x.split(".")[0]}")
df["url"] = df["url"].fillna("")
for col in df.columns:
    if df[col].dtype == str:
        df[col] = df[col].apply(lambda x: x.strip())

logger.info("Dataframe cleaning complete.")


# Define transform specifications
transform_specs = {
    "regions": {
        "data": lambda df: (
            df[["region"]]
            .drop_duplicates()
            .sort_values(by=["region"])
            .reset_index(drop=True)
            .rename_axis("_id")
            .reset_index()
            ),
        "properties": lambda doc: {
            "_id": doc["_id"],
            "name": doc["region"],
            "updated_at": doc["updated_at"]
            }
    },
    "local_authorities": {
        "data": lambda df: (
            df[["local_authority", "region"]]
            .dropna(subset=["local_authority"]).drop_duplicates()
            .sort_values(by=["region", "local_authority"])
            .reset_index(drop=True)
            .rename_axis("_id")
            .reset_index()
            ),
        "properties": lambda doc: {
            "_id": doc["_id"],
            "name": doc["local_authority"],
            "region": doc["region"],
            "updated_at": doc["updated_at"]
            },
    },
    "postcodes": {
        "data": lambda df: (
            df[["postcode", "local_authority", "region"]]
            .dropna(subset=["postcode"]).drop_duplicates()
            .sort_values(by=["region", "local_authority"])
            .reset_index(drop=True)
            .rename_axis("_id")
            .reset_index()
            ),
        "properties": lambda doc: {
            "_id": doc["_id"],
            "postcode": doc["postcode"],
            "local_authority": doc["local_authority"],
            "region": doc["region"],
            "updated_at": doc["updated_at"]
            }
        },
    "providers": {
        "data": lambda df: (
            df[["provider", "provider_id"]]
            .dropna(subset=["provider"]).drop_duplicates()
            .sort_values(by=["provider"])
        ),
        "properties": lambda doc: {
            "_id": doc["provider_id"],
            "name": doc["provider"],
            "updated_at": doc["updated_at"]
            }
        },
    "locations": {
        "data": lambda df: (
            df
            .dropna(subset=["location_id", "provider_id"]).drop_duplicates()
            .sort_values(by=["region", "name"])
        ),
        "properties": lambda doc: {
            "_id": doc["location_id"].strip(),
            "name": doc["name"].strip(),
            "address": ", ".join([i.strip() for i in doc["address"].split(",") if i.strip()]),
            "postcode": doc["postcode"].strip(),
            "phone_no": doc["phone_no"].strip(),
            "url": doc["url"],
            "services": sorted({s.strip() for s in str(doc["services"]).split("|")}),
            "service_types": sorted({s.strip() for s in str(doc["service_types"]).split("|")}),
            "provider": doc["provider"].strip(),
            "provider_id": doc["provider_id"].strip(),
            "cqc_url": doc["location_url"].strip(),
            "latest_check_date": doc["latest_check_date"].isoformat(),
            "updated_at": doc["updated_at"]
            }
        },
    "services": {
        "data": lambda df: (
            pd.DataFrame(decompose_list(list(df["services"].unique()), "|"), columns=["name"])
            .sort_values(by="name")
            .reset_index(drop=True)
            .rename_axis("_id")
            .reset_index()
        ),
        "properties": lambda doc: {
            "_id": doc["_id"],
            "name": doc["name"],
            "updated_at": doc["updated_at"]
            }
        },
    "service_types": {
        "data": lambda df: (
            pd.DataFrame(decompose_list(list(df["service_types"].unique()), "|"), columns=["name"])
            .sort_values(by="name")
            .reset_index(drop=True)
            .rename_axis("_id")
            .reset_index()
        ),
        "properties": lambda doc: {
            "_id": doc["_id"],
            "name": doc["name"],
            "updated_at": doc["updated_at"]
            }
        },
}

if __name__ == "__main__":
    make_documents(
        df=df,
        transform_specs=transform_specs,
        ts=timestamp,
        output_dir=PROCESSED_DATA_DIR
        )
