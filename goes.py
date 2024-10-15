import functools
import io
import pandas as pd
import requests
import xarray as xr

GOES_MAG_BASE_URL = (
    "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes"
)

SATELLITE = ["goes16", "goes17", "goes18"]


def goes16_magnetometer(date: pd.Timestamp | str) -> xr.DataArray:
    date = pd.Timestamp(date)
    ds = open_magnetometer_ds(date, "goes16")
    return ds.b_epn


def goes17_magnetometer(date: pd.Timestamp | str) -> xr.DataArray:
    date = pd.Timestamp(date)
    ds = open_magnetometer_ds(date, "goes17")
    return ds.b_epn


def goes18_magnetometer(date: pd.Timestamp | str) -> xr.DataArray:
    date = pd.Timestamp(date)
    ds = open_magnetometer_ds(date, "goes18")
    return ds.b_epn


@functools.cache
def open_magnetometer_ds(date: pd.Timestamp, sat: str) -> xr.Dataset:
    url = magnetometer_data_url(date, sat)
    response = requests.get(url)
    response.raise_for_status()

    fileobj = io.BytesIO(response.content)
    return xr.open_dataset(fileobj)


def magnetometer_data_url(date: pd.Timestamp, sat: str):
    if not sat in SATELLITE:
        raise ValueError(f"Invalid satellite: {sat}")
    sat_abbr = sat.replace("goes", "g")
    return f"{GOES_MAG_BASE_URL}/{sat}/l2/data/magn-l2-avg1m/{date:%Y}/{date:%m}/dn_magn-l2-avg1m_{sat_abbr}_d{date:%Y%m%d}_v2-0-2.nc"
