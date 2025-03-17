import os
import unittest.mock as mock
from pathlib import Path

import tomllib

from api_tabular import Configurator

builtin_open = open


# Mock custom config.toml with specific PAGE_SIZE_MAX value
def mock_open_with_custom_config_toml(*args, **kwargs):
    if args[0].name == "config.toml":
        # mocked open for path "config.toml"
        return mock.mock_open(read_data=b"PAGE_SIZE_MAX = 200")(*args, **kwargs)
    # unpatched version for every other path
    return builtin_open(*args, **kwargs)


def test_default_config():
    config = Configurator()

    assert config.PAGE_SIZE_MAX == 50

    # Make sure all config keys are defined
    with open(Path(__file__).parent.parent / "api_tabular/config_default.toml", "rb") as f:
        assert config.configuration.keys() == tomllib.load(f).keys()


@mock.patch("builtins.open", mock_open_with_custom_config_toml)
def test_custom_config_file_override():
    config = Configurator()

    assert config.PAGE_SIZE_MAX == 200


def test_env_override():
    os.environ["PGREST_ENDPOINT"] = "https://example.com"
    os.environ["PAGE_SIZE_MAX"] = "200"
    os.environ["ALLOW_AGGREGATION"] = "a,b,c"
    config = Configurator()

    assert config.PGREST_ENDPOINT == "https://example.com"
    assert config.PAGE_SIZE_MAX == 200
    assert config.ALLOW_AGGREGATION == ["a", "b", "c"]
