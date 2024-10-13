import functools
import pandas as pd
import requests

ACE_BASE_URL = "https://sohoftp.nascom.nasa.gov/sdb/goes/ace"


def open_ace(dt: pd.Timestamp | str, mask: bool = True) -> pd.DataFrame:
    dt = pd.Timestamp(dt)
    mag = ace_daily_text(dt, "mag")
    swepam = ace_daily_text(dt, "swepam")

    df_mag = parse_ace_magnetometer(mag)
    df_plasma = parse_ace_plasma(swepam)

    result = df_mag.merge(df_plasma, on=["datetime"], suffixes=["_mag", "_swepam"])
    result.set_index(["datetime"], inplace=True)
    if mask:
        for column in result.columns:
            result[column] = result[column].mask(result[column] < -999)
    return result


@functools.cache
def ace_daily_text(dt: pd.Timestamp, product: str) -> list[str]:
    url = f"{ACE_BASE_URL}/daily/{dt:%Y%m%d}_ace_{product}_1m.txt"
    response = requests.get(url)
    response.raise_for_status()

    lines = response.text.split("\n")
    return [line for line in lines if line and line[0] not in ["#", ":"]]


def parse_ace_magnetometer(text: list[str]) -> pd.DataFrame:
    rows = []
    for line in text:
        tokens = line.split()
        year, month, day, time = tuple(tokens[:4])
        hour = int(time[:2])
        minute = int(time[2:])
        ts = pd.Timestamp(
            year=int(year), month=int(month), day=int(day), hour=hour, minute=minute
        )
        # qc = tokens[6]
        bx, by, bz, bt = tuple(tokens[7:11])
        rows.append(dict(datetime=ts, bt=bt, bx=bx, by=by, bz=bz))
    df = pd.DataFrame(rows)
    return df.astype({"bx": float, "by": float, "bz": float, "bt": float})


def parse_ace_plasma(text: list[str]) -> pd.DataFrame:
    rows = []
    for line in text:
        tokens = line.split()
        year, month, day, time = tuple(tokens[:4])
        hour = int(time[:2])
        minute = int(time[2:])
        ts = pd.Timestamp(
            year=int(year), month=int(month), day=int(day), hour=hour, minute=minute
        )
        # qc = tokens[6]
        density, speed, temperature = tuple(tokens[7:])
        rows.append(
            dict(
                datetime=ts,
                density=float(density),
                speed=float(speed),
                temperature=float(temperature),
            )
        )
    df = pd.DataFrame(rows)
    return df.astype({"density": float, "speed": float, "temperature": float})
