import pandas as pd


def merge_dfs_mean(
    df1: pd.DataFrame, df2: pd.DataFrame, cols: list[str] | None = None
) -> pd.DataFrame:
    if cols is None:
        cols = ["bt", "bx", "by", "bz", "speed", "density", "temperature"]

    res = df1.copy()
    for col in cols:
        combined = pd.concat([df1[col], df2[col]], axis=1)
        res[col] = combined.mean(axis=1)

    return res[cols]
