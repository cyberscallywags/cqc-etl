"""Transform CQC directory data"""

# Imports
import json
from datetime import datetime
import pandas as pd
from loguru import logger
from src.utils.parsers import decompose_list, clean_document
from src.utils.transforms import combine_csvs, add_timestamp
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

timestamp = datetime.now()

df = combine_csvs(RAW_DATA_DIR / "directory")
df.columns = [
    "name", "aka", "address", "postcode", "phone_no",
    "url", "service_types", "latest_check_date", "services",
    "provider", "local_authority", "region",
    "location_url", "location_id", "provider_id"
    ]
df["latest_check_date"] = pd.to_datetime(df["latest_check_date"])
df["phone_no"] = df["phone_no"].astype(str)
df["phone_no"] = df["phone_no"].apply(lambda x: "" if x == "nan" else f"0{x.split(".")[0]}")
df["url"] = df["url"].fillna("")
df["provider"] = df["provider"].apply(lambda x: x.strip())

# Create regions JSON
r_df = df[["region"]].copy().drop_duplicates()
r_df.columns = ["name"]
r_df = r_df.dropna().sort_values(by="name").reset_index(names=["_id"])

# Create local authorities JSON
la_df = df[["local_authority", "region"]].copy().drop_duplicates()
la_df.columns = ["name", "region"]
la_df = la_df[~la_df["name"].isna()]
la_df = la_df.sort_values(by=["region", "name"]).reset_index(names=["_id"])

# Create postcodes JSON
pc_df = df[["postcode", "local_authority", "region"]].copy().drop_duplicates()
pc_df = pc_df.dropna(subset=["postcode"])
pc_df = pc_df.sort_values(by=["region", "local_authority"]).reset_index(names=["_id"])

# Create providers JSON
p_df = df[["provider", "provider_id"]].copy().drop_duplicates()
p_df.columns = ["name", "_id"]
p_df = p_df.dropna(subset=["name"])
p_df = p_df.sort_values(by=["name"]).reset_index(drop=True)

# Create services JSON
s_df = pd.DataFrame(decompose_list(list(df["services"].unique()), "|"), columns=["name"])
s_df = s_df.sort_values(by="name").reset_index(names=["_id"])

# Create service types JSON
st_df = pd.DataFrame(decompose_list(list(df["service_types"].unique()), "|"), columns=["name"])
st_df = st_df.sort_values(by="name").reset_index(names=["_id"])

# Create JSON files
file_map = {
    "regions": r_df,
    "local_authorities": la_df,
    "postcodes": pc_df,
    "services": s_df,
    "service_types": st_df
}

for filename, mod_df in file_map.items():
    mod_df = add_timestamp(mod_df, timestamp)
    mod_df.to_json(PROCESSED_DATA_DIR / f"{filename}.json", indent=2, orient='records')
    logger.info(f"Saved {filename}.json ({len(mod_df)} records) to {PROCESSED_DATA_DIR}")

# Create locations JSON
locations = list(df.to_dict(orient="records"))
r_locations = []

for location in locations:
    r_location = {
        "_id": location["location_id"].strip(),
        "name": location["name"].strip(),
        "address": ", ".join([i.strip() for i in location["address"].split(",")]),
        "postcode": location["postcode"].strip(),
        "phone_no": location["phone_no"].strip(),
        "url": location["url"],
        "services": sorted({s.strip() for s in str(location["services"]).split("|")}),
        "service_types": sorted({s.strip() for s in str(location["service_types"]).split("|")}),
        "provider": location["provider"].strip(),
        "provider_id": location["provider_id"].strip(),
        "cqc_url": location["location_url"].strip(),
        "latest_check_date": location["latest_check_date"].isoformat(),
        "updated_at": timestamp.isoformat(),
    }
    r_location, _ = clean_document(r_location)
    r_locations.append(r_location)

with open(PROCESSED_DATA_DIR / "locations.json", "w", encoding="utf-8") as f:
    json.dump(r_locations, f, indent=2)
logger.info(f"Saved locations.json JSON saved to {PROCESSED_DATA_DIR}")
