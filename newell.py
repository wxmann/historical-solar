import numpy as np
import numpy.typing as npt


def newell_coupling(
    by: npt.ArrayLike, bz: npt.ArrayLike, v: npt.ArrayLike
) -> npt.ArrayLike:
    bT = np.sqrt(by**2 + bz**2)
    tc = imf_clock_angle(by, bz)
    sintc = np.abs(np.sin(tc / 2))
    return (v ** (4 / 3)) * (sintc ** (8 / 3)) * (bT ** (2 / 3))


def imf_clock_angle(by: npt.ArrayLike, bz: npt.ArrayLike) -> npt.ArrayLike:
    # bT = np.sqrt(by**2 + bz**2)
    return np.arctan2(by, bz)
    # TODO: why do we need this?
    # neg_tc = (bT * np.cos(tc) * bz) < 0
    # tc[neg_tc] = tc[neg_tc] + np.pi
    # return  bT,tc
