"""
Diff utilities for comparing file summaries and tracking changes.
"""

import pandas as pd
import numpy as np


def diff_summaries(df_old: pd.DataFrame, df_new: pd.DataFrame, count_col: str = "files_created") -> pd.DataFrame:
    """
    Compare two file summary DataFrames and return a diff showing changes.
    
    Args:
        df_old: Previous file summary DataFrame
        df_new: Current file summary DataFrame
        count_col: Column name containing the count to compare
        
    Returns:
        DataFrame with diff showing ADDED, REMOVED, CHANGED, and UNCHANGED files
    """
    def norm(df):
        d = df.copy()
        d["_prefix"]      = d["prefix"].astype("string")
        d["_file_type"]   = d["file_type"].astype("string")
        d["_file_format"] = d.get("file_format").astype("string").fillna("__NULL__")
        d["_minute_str"]  = pd.to_datetime(d["created_minute"], errors="coerce").dt.floor("min").dt.strftime("%Y-%m-%d %H:%M:00")
        return d

    keys = ["_prefix","_file_type","_file_format","_minute_str"]

    oldN = norm(df_old)
    newN = norm(df_new)

    # aggregate + keep natural fields on each side
    oldA = (oldN.groupby(keys, dropna=False)
                 .agg(old_count=(count_col,"sum"),
                      prefix=("prefix","first"),
                      file_type=("file_type","first"),
                      file_format=("file_format","first"),
                      created_minute=("created_minute","first"))
                 .reset_index())

    newA = (newN.groupby(keys, dropna=False)
                 .agg(new_count=(count_col,"sum"),
                      prefix=("prefix","first"),
                      file_type=("file_type","first"),
                      file_format=("file_format","first"),
                      created_minute=("created_minute","first"))
                 .reset_index())

    # present in both
    both = newA.merge(oldA[keys + ["old_count"]], on=keys, how="inner")
    both = both.loc[:, ["prefix","file_type","file_format","created_minute","old_count","new_count"]]
    both["delta"] = both["new_count"] - both["old_count"]
    both["status"] = np.where(both["delta"].eq(0), "UNCHANGED", "CHANGED")

    # added (in new only)
    added = newA.merge(oldA[keys], on=keys, how="left", indicator=True)
    added = (added[added["_merge"] == "left_only"]
                  .drop(columns="_merge")
                  .assign(old_count=0)[["prefix","file_type","file_format","created_minute","old_count","new_count"]])
    added["delta"] = added["new_count"]
    added["status"] = "ADDED"

    # removed (in old only)
    removed = oldA.merge(newA[keys], on=keys, how="left", indicator=True)
    removed = (removed[removed["_merge"] == "left_only"]
                    .drop(columns="_merge")
                    .assign(new_count=0)[["prefix","file_type","file_format","created_minute","old_count","new_count"]])
    removed["delta"] = -removed["old_count"]
    removed["status"] = "REMOVED"

    # union + prettify
    diff = pd.concat([both, added, removed], ignore_index=True)
    diff["minute_str"] = pd.to_datetime(diff["created_minute"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    return (diff[["prefix","file_type","file_format","created_minute","minute_str",
                  "old_count","new_count","delta","status"]]
            .sort_values(["prefix","file_type","file_format","created_minute"])
            .reset_index(drop=True))
