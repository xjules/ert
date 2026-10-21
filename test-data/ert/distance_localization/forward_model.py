#!/usr/bin/env python3
"""Toy forward model for the distance localization example.

On the first iteration it samples one standard normal value per realization,
fills PORO uniformly with it and writes it to poro.bgrdecl. On later iterations ERT provides the updated
poro.bgrdecl. Each well response is the mean PORO of the 3 x 3 cells around it,
written as a WOPR summary vector.
"""

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import resfo
from resfo_utilities.testing import (
    Date,
    Simulator,
    Smspec,
    SmspecIntehead,
    SummaryMiniStep,
    SummaryStep,
    UnitSystem,
    Unsmry,
)

NX, NY, CELL_SIZE = 40, 40, 1.0
START_DATE = dt.date(2019, 12, 1)
RESPONSE_DATE = dt.date(2020, 1, 1)

WELLS = {
    "OP1": (10.5, 10.5),
    "OP2": (29.5, 15.5),
    "OP3": (15.5, 30.5),
}


def load_or_sample_poro(iens: int) -> np.ndarray:
    path = Path("poro.bgrdecl")
    if path.exists():
        return resfo.read(path)[0][1].reshape(NX, NY, order="F")
    value = np.random.default_rng(iens).standard_normal()
    poro = np.full((NX, NY), value, dtype=np.float32)
    resfo.write(path, [("PORO    ", poro.flatten(order="F"))])
    return poro


def well_response(poro: np.ndarray, east: float, north: float) -> float:
    i, j = int(east // CELL_SIZE), int(north // CELL_SIZE)
    return float(poro[i - 1 : i + 2, j - 1 : j + 2].mean())


def write_summary(values: dict[str, float]) -> None:
    hours = 24 * (RESPONSE_DATE - START_DATE).days
    unsmry = Unsmry(
        steps=[
            SummaryStep(
                seqnum=0,
                ministeps=[
                    SummaryMiniStep(mini_step=0, params=[hours, *values.values()])
                ],
            )
        ]
    )
    smspec = Smspec(
        nx=NX,
        ny=NY,
        nz=1,
        restarted_from_step=0,
        num_keywords=1 + len(values),
        restart="        ",
        keywords=["TIME    ", *(["WOPR    "] * len(values))],
        well_names=[":+:+:+:+", *(well.ljust(8) for well in values)],
        region_numbers=[-32676, *([0] * len(values))],
        units=["HOURS   ", *(["SM3"] * len(values))],
        start_date=Date.from_datetime(dt.datetime.combine(START_DATE, dt.time())),
        intehead=SmspecIntehead(
            unit=UnitSystem.METRIC,
            simulator=Simulator.ECLIPSE_100,
        ),
    )
    smspec.to_file("CASE.SMSPEC")
    unsmry.to_file("CASE.UNSMRY")


if __name__ == "__main__":
    poro = load_or_sample_poro(int(sys.argv[1]))
    write_summary(
        {
            well: well_response(poro, east, north)
            for well, (east, north) in WELLS.items()
        }
    )
