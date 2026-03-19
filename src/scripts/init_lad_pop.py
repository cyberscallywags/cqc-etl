"""Make LAD population by age time series CSV."""

# Imports
import pandas as pd
from src.config import CORE_DATA_DIR
from src.utils.parsers import get_latest_code

# Load data
lad_pop = pd.read_csv(CORE_DATA_DIR / "init/lad_population_by_age_group_full_ts.csv")
lad_15 = pd.read_csv(CORE_DATA_DIR / "lookups/LAD_(April_2015)_Names_and_Codes_in_the_United_Kingdom.csv")
lad_23 = pd.read_csv(CORE_DATA_DIR / "lookups/Local_Authority_Districts_(April_2023)_Names_and_Codes_in_the_United_Kingdom.csv")
lad_25 = pd.read_csv(CORE_DATA_DIR / "lookups/LAD_MAY_2025_UK_BUC_4725703192843186948.csv")
changes = pd.read_csv(CORE_DATA_DIR / "code_history/Changes.csv")

# Clean data
lad_pop = lad_pop.replace({"Rhondda Cynon Taff": "Rhondda Cynon Taf"})
lad_pop = lad_pop.rename(columns={"ladnm": "LADNM", "total": "TOTAL", "year": "YEAR"})
lad_15.loc[len(lad_15)] = [len(lad_15)+2, "E07000112", "Folkestone and Hythe"]

# Make population subsets
pop_15 = lad_pop[lad_pop["YEAR"] < 2021].copy()
pop_15["LAD_YR"] = 2015
pop_23 = lad_pop[lad_pop["YEAR"] == 2021].copy()
pop_23["LAD_YR"] = 2023

# Merge in LADCDs from lad_15 and lad_23
pop_15 = pop_15.merge(
    lad_15[["LAD15CD", "LAD15NM"]],
    left_on="LADNM",
    right_on="LAD15NM",
    how="left"
)

pop_23 = pop_23.merge(
    lad_23[["LAD23CD", "LAD23NM"]],
    left_on="LADNM",
    right_on="LAD23NM",
    how="left"
)

# Make successor map from changes
successor_map = (
    changes[changes["GEOGCD_P"].notna()][["GEOGCD_P", "GEOGCD"]]
    .drop_duplicates()
    .set_index("GEOGCD_P")["GEOGCD"]
    .to_dict()
)

# Get latest codes and merge into dfs
pop_15["lad_latest"] = pop_15["LAD15CD"].apply(lambda c: get_latest_code(c, successor_map))
pop_23["lad_latest"] = pop_23["LAD23CD"].apply(lambda c: get_latest_code(c, successor_map))

pop_15 = pop_15.merge(
    lad_25[["LAD25CD", "LAD25NM"]],
    left_on="lad_latest",
    right_on="LAD25CD",
    how="left"
)

pop_23 = pop_23.merge(
    lad_25[["LAD25CD", "LAD25NM"]],
    left_on="lad_latest",
    right_on="LAD25CD",
    how="left"
)

lad_pop_new = pd.concat([pop_15, pop_23])[[
    "LADNM", "LAD25NM", "LAD25CD", "YEAR", "TOTAL",
    "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84", "85-89", "90+"
]]

# Check merges and splits
check_ms = lad_pop_new.groupby("LAD25CD").size().loc[lambda x: x > 4].reset_index(name="count")
to_merge = (
    lad_pop_new
    .groupby(["LAD25CD", "YEAR"])["LADNM"]
    .nunique()
    .reset_index(name="n_names")
    .query("n_names > 1")
)

merged = (
    lad_pop_new
    .groupby(["LAD25CD", "LAD25NM", "YEAR"], as_index=False)
    .agg({
        "TOTAL": "sum",
        "50-54": "sum",
        "55-59": "sum",
        "60-64": "sum",
        "65-69": "sum",
        "70-74": "sum",
        "75-79": "sum",
        "80-84": "sum",
        "85-89": "sum",
        "90+": "sum"
    })
)

merged.to_csv(CORE_DATA_DIR / "lookups/LAD Populations by Age (1991-2021).csv", index=False)
