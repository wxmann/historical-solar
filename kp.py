import functools
import pandas as pd

KP_URL = "https://www-app3.gfz-potsdam.de/kp_index/Kp_ap_since_1932.txt"

KP_DATA_COLUMNS = [
    "year",
    "month",
    "day",
    "hr",
    "hr_min",
    "day_dec",
    "day_dec_min",
    "Kp",
    "ap",
    "is_definitive",
]


@functools.cache
def kp_indices() -> pd.DataFrame:
    df = pd.read_csv(KP_URL, sep=r"\s+", names=KP_DATA_COLUMNS, header=0, comment="#")
    df["hr"] = df.hr.astype(int)
    return df[["year", "month", "day", "hr", "Kp", "ap", "is_definitive"]]
