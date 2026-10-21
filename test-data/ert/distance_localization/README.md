# Distance based localization - minimal example

A small version of the `distance_localization` notebook in the docs that can be
run from the ERT GUI:

```
cd test-data/ert/distance_localization
ert gui config.ert
```

Run the *Ensemble smoother* experiment mode.

- A 40 x 40 grid with 1 m cells and one `FIELD` parameter, `PORO`.
- `PORO` is spatially uniform in each realization, with the value drawn from a
  standard normal distribution by the forward model (`FORWARD_INIT:True`).
  The realizations differ only in this one value.
- Three wells; each response (`WOPR:OP1`, `WOPR:OP2`, `WOPR:OP3`) is the mean
  `PORO` of the 3 x 3 cells around the well.
- Each observation in `observations.txt` has a `LOCALIZATION` block
  (`EAST`, `NORTH`, `RADIUS`).

`config.ert` uses `ANALYSIS_SET_VAR PARAMETERS FIELD DISTANCE`. Change it to
`GLOBAL` to compare: with `DISTANCE` only cells within `2 x RADIUS` of a well
are updated, whereas `GLOBAL` changes the whole field.

`generate_grid.py` was used once to create `GRID.EGRID`.
