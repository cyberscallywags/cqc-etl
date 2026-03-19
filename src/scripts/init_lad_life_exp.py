"""Make life expectancy by LAD time series dataset"""

# Imports
import polars as pl
from src.config import CORE_DATA_DIR
from src.utils.parsers import get_latest_code


# Load data
df = pl.read_csv(CORE_DATA_DIR / "init/life-expectancy-by-local-authority-time-series-v1.csv")
lad_25 = pl.read_csv(CORE_DATA_DIR / "lookups/LAD_MAY_2025_UK_BUC_4725703192843186948.csv")
changes = pl.read_csv(CORE_DATA_DIR / "code_history/Changes.csv")


# Mod changes data and create successor map
changes = changes.with_columns(
    pl.col("OPER_DATE").str.strptime(pl.Datetime)
)
changes = changes.sort("OPER_DATE").drop_nulls(subset=["GEOGCD", "GEOGCD_P"])
successor_map = (
    changes
    .filter(pl.col("GEOGCD_P").is_not_null())
    .select(["GEOGCD_P", "GEOGCD"])
    .unique()
    .to_dict(as_series=False)
)

successor_map = dict(zip(successor_map["GEOGCD_P"], successor_map["GEOGCD"]))


# Clean df data and join latest LAD codes and names
df = df.drop(["AgeGroups", "Sex", "two-year-intervals"])
df.columns = [
    "AVG", "LOWER_CI", "UPPER_CI", "INTERVAL",
    "LADCD", "LADNM", "SEX", "AGE_GROUPS"
    ]

df = df.filter(
    pl.col("AGE_GROUPS")
    .str.extract(r"^(\d+)")
    .cast(pl.Int64) >= 50
    )

df = df.with_columns(
    pl.col("INTERVAL").str.slice(0, 4).cast(pl.Int32).alias("START_YEAR")
    )

df = df.with_columns(
    pl.col("LADCD")
    .map_elements(lambda c: get_latest_code(c, successor_map))
    .alias("LAD25CD")
    )

df = (
    df.join(lad_25[["LAD25CD", "LAD25NM"]], on="LAD25CD", how="left")
    .sort(["LAD25CD", "INTERVAL"])
    )

df = df.with_columns(
    pl.when(pl.col("LAD25NM").is_null(), pl.col("LAD25CD") == pl.col("LADCD"))
    .then(pl.col("LADNM"))
    .otherwise(pl.col("LAD25NM"))
    .alias("LAD25NM")
    )


# Pivot dataframe on age groups and save
age_groups = (
    df["AGE_GROUPS"]
    .unique()
    .sort()
    .to_list()
)

pivot_df = df.drop_nulls(subset=["LAD25NM"]).pivot(
    on="AGE_GROUPS",
    values=["AVG", "LOWER_CI", "UPPER_CI"],
    index=["LAD25CD", "LAD25NM", "START_YEAR", "INTERVAL"],
    aggregate_function="mean",
    sort_columns=True
)

# Create list columns
pivot_df = pivot_df.with_columns([
    pl.concat_list([
        pl.col(f"AVG_{age}"),
        pl.col(f"LOWER_CI_{age}"),
        pl.col(f"UPPER_CI_{age}")
    ]).alias(f"{age}")
    for age in age_groups
])

# Drop intermediate columns
cols_to_drop = [
    f"{prefix}_{age}"
    for prefix in ["AVG", "LOWER_CI", "UPPER_CI"]
    for age in age_groups
]

pivot_df = pivot_df.drop(cols_to_drop).sort(["LAD25CD", "INTERVAL"])
pivot_df.write_parquet(CORE_DATA_DIR / "lookups/Life Expectancy by LAD ts.parquet")
