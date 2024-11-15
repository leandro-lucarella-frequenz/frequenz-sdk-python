# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the config utilities."""

import dataclasses
from typing import Any

import marshmallow
import marshmallow_dataclass
import pytest
from pytest_mock import MockerFixture

from frequenz.sdk.config._util import load_config


@dataclasses.dataclass
class SimpleConfig:
    """A simple configuration class for testing."""

    name: str
    value: int


@marshmallow_dataclass.dataclass
class MmSimpleConfig:
    """A simple configuration class for testing."""

    name: str = dataclasses.field(metadata={"validate": lambda s: s.startswith("test")})
    value: int


def test_load_config_dataclass() -> None:
    """Test that load_config loads a configuration into a configuration class."""
    config: dict[str, Any] = {"name": "test", "value": 42}

    loaded_config = load_config(SimpleConfig, config)
    assert loaded_config == SimpleConfig(name="test", value=42)

    config["name"] = "not test"
    loaded_config = load_config(SimpleConfig, config)
    assert loaded_config == SimpleConfig(name="not test", value=42)


def test_load_config_marshmallow_dataclass() -> None:
    """Test that load_config loads a configuration into a configuration class."""
    config: dict[str, Any] = {"name": "test", "value": 42}
    loaded_config = load_config(MmSimpleConfig, config)
    assert loaded_config == MmSimpleConfig(name="test", value=42)

    config["name"] = "not test"
    with pytest.raises(marshmallow.ValidationError):
        _ = load_config(MmSimpleConfig, config)


def test_load_config_load_None() -> None:
    """Test that load_config raises ValidationError if the configuration is None."""
    config: dict[str, Any] = {}
    with pytest.raises(marshmallow.ValidationError):
        _ = load_config(MmSimpleConfig, config.get("loggers", None))


def test_load_config_type_hints(mocker: MockerFixture) -> None:
    """Test that load_config loads a configuration into a configuration class."""
    mock_class_schema = mocker.Mock()
    mock_class_schema.return_value.load.return_value = {"name": "test", "value": 42}
    mocker.patch(
        "frequenz.sdk.config._util.class_schema", return_value=mock_class_schema
    )
    config: dict[str, Any] = {}

    # We add the type hint to test that the return type (hint) is correct
    _: MmSimpleConfig = load_config(MmSimpleConfig, config, marshmallow_arg=1)
    mock_class_schema.return_value.load.assert_called_once_with(
        config, marshmallow_arg=1
    )
