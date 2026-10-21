"""Generates GRID.EGRID: a 40 x 40 x 1 box grid with 1 m cells."""

import xtgeo

xtgeo.create_box_grid((40, 40, 1), increment=(1.0, 1.0, 1.0)).to_file(
    "GRID.EGRID", fformat="egrid"
)
