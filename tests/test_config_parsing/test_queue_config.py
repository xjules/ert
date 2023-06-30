import hypothesis.strategies as st
import pytest
from hypothesis import given

from ert.config import ConfigValidationError, ErtConfig
from ert.job_queue import Driver


@pytest.mark.usefixtures("use_tmpdir", "set_site_config")
@given(st.integers(min_value=1, max_value=300))
def test_queue_config_default_max_running_is_unlimited(num_real):
    filename = "config.ert"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"NUM_REALIZATIONS {num_real}\nQUEUE_SYSTEM SLURM\n")
    # max_running == 0 means unlimited
    assert (
        Driver.create_driver(ErtConfig.from_file(filename).queue_config).max_running
        == 0
    )


@pytest.mark.usefixtures("use_tmpdir", "set_site_config")
@given(st.integers(min_value=1, max_value=300))
def test_queue_config_invalid_queue_system_provided(num_real):
    filename = "config.ert"

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"NUM_REALIZATIONS {num_real}\nQUEUE_SYSTEM VOID\n")

    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="Invalid QUEUE_SYSTEM provided: 'VOID'",
    ):
        _ = ErtConfig.from_file(filename)
