from typing import Literal
import pandas as pd

from ace import open_ace
from dscovr import open_dscovr
from merge import merge_dfs_mean


def geomagnetic_storm_data(
    date: pd.Timestamp | str,
    satellite: Literal["ace", "dscovr", "both"] = "both",
    before_shock: int = 3,
    after_shock: int = 24,
    shock_thresholds: tuple[float, float] = (5, 50),
) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    dt = pd.Timestamp(date).replace(hour=0, minute=0, second=0)
    dt_min = dt - pd.Timedelta(days=before_shock // 24 + 1)
    dt_max = dt + pd.Timedelta(days=after_shock // 24 + 1)
    dt_range = pd.date_range(dt_min, dt_max, freq="D")

    def _open_data(dt):
        if satellite == "ace":
            return open_ace(dt)
        elif satellite == "dscovr":
            return open_dscovr(dt)
        elif satellite == "both":
            df_ace = open_ace(dt)
            df_dscovr = open_dscovr(dt)
            return merge_dfs_mean(df_ace, df_dscovr)
        else:
            raise ValueError(f"Invalid satellite: {satellite}")

    dfs = [_open_data(dt) for dt in dt_range]
    df = pd.concat(dfs)

    try:
        diff_threshold, pct_change_threshold = shock_thresholds
        # only take the earliest shock
        shock = detect_shocks(
            df,
            "bt",
            diff_threshold=diff_threshold,
            pct_change_threshold=pct_change_threshold,
        ).index[0]
    except IndexError:
        # no shock found
        return df, None

    init_time = shock - pd.Timedelta(hours=before_shock)
    final_time = shock + pd.Timedelta(hours=after_shock)
    return df.loc[init_time:final_time], shock


def detect_shocks(
    df: pd.DataFrame,
    column: str = "bt",
    diff_threshold: float = 5,
    pct_change_threshold: float = 50,
    rolling_window_check: str | None = "30min",
) -> pd.DataFrame:
    df = df.copy()  # work on a different dataframe
    df["Difference"] = df[column].diff()

    df["Rate_of_Change"] = df[column].pct_change() * 100  # Convert to percentage
    df["Shock"] = (df["Rate_of_Change"] > pct_change_threshold) & (
        df["Difference"] > diff_threshold
    )
    shocks = df[df.Shock]

    if rolling_window_check and len(shocks):
        df_roll = df.rolling(rolling_window_check).mean()

        def _check_rolling(row):
            dt = row.name
            bt_roll_prior = df_roll[df_roll.index == dt].iloc[0][column]
            bt_roll_after = df_roll[
                df_roll.index == dt + pd.Timedelta(rolling_window_check)
            ].iloc[0][column]

            return (
                bt_roll_after > bt_roll_prior + diff_threshold
                and bt_roll_after > bt_roll_prior * pct_change_threshold / 100
            )

        return shocks[shocks.apply(_check_rolling, axis=1)]

    return shocks
