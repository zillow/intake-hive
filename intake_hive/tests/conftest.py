from pathlib import Path

import pytest


@pytest.fixture
def catalog_path():
    return str(Path(__file__).resolve().parent.joinpath(Path("catalog.yaml")))


@pytest.fixture
def dal_catalog_path():
    return str(Path(__file__).resolve().parent.joinpath(Path("dal-catalog.yaml")))
