import functools
import pandas as pd
import xarray as xr
import requests
import gzip
import io
from bs4 import BeautifulSoup

DSCOVR_PARENT_URL = "https://www.ngdc.noaa.gov/dscovr/data"

MAGNOMETER_COLUMNS = ["bt", "bx_gsm", "by_gsm", "bz_gsm", "phi_gsm"]
FARADAY_COLUMNS = ["proton_speed", "proton_density", "proton_temperature"]

RENAME_COLUMNS = dict(
    **{c: c.replace("_gsm", "") for c in MAGNOMETER_COLUMNS},
    **{c: c.replace("proton_", "") for c in FARADAY_COLUMNS},
)


def open_dscovr(dt: pd.Timestamp | str) -> pd.DataFrame:
    try:
        df_mag = open_dscovr_dataset(dt, "m1m").to_dataframe()
        df_plasma = open_dscovr_dataset(dt, "f1m").to_dataframe()
    except (requests.exceptions.HTTPError, ValueError):
        return pd.DataFrame(columns=MAGNOMETER_COLUMNS + FARADAY_COLUMNS)

    df_bfield = df_mag[MAGNOMETER_COLUMNS]
    df_protons = df_plasma[FARADAY_COLUMNS]

    df_final = df_bfield.join(df_protons)
    return df_final.rename(columns=RENAME_COLUMNS)


@functools.cache
def open_dscovr_dataset(dt: pd.Timestamp | str, product: str = "m1m") -> xr.Dataset:
    dt = pd.Timestamp(dt).replace(minute=0, second=0)
    url = dscovr_dataset_link(dt, product)
    if not url:
        raise ValueError(
            f"Cannot find dataset at {dt.isoformat()} and product {product}"
        )

    response = requests.get(url)
    response.raise_for_status()

    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        with io.BytesIO(gz.read()) as decompressed_file:
            return xr.open_dataset(decompressed_file)


def dscovr_dataset_link(dt: pd.Timestamp, product: str) -> str:
    url = f"{DSCOVR_PARENT_URL}/{dt:%Y}/{dt:%m}"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for a in soup.find_all("a", href=True):
        link = a.get("href", None)
        conditions = [
            f"{product}_dscovr" in link,
            f"s{dt:%Y%m%d}" in link,
            f"e{dt:%Y%m%d}" in link,
        ]
        if link and all(conditions):
            if not link.startswith("http"):
                link = f"{url}/{link}"
            return link
    return None
