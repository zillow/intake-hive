from pathlib import Path

import pytest

import intake


@pytest.fixture
def catalog_path():
    return str(Path(__file__).resolve().parent.joinpath(Path("catalog.yaml")))


@pytest.fixture
def cat(catalog_path: str):
    return intake.Catalog(catalog_path)
