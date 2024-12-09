# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for logging config updater."""

import asyncio
import logging
from typing import Any

import pytest
from frequenz.channels import Broadcast
from marshmallow import ValidationError
from pytest_mock import MockerFixture

from frequenz.sdk.config import (
    LoggerConfig,
    LoggingConfig,
    LoggingConfigUpdatingActor,
    load_config,
)


def test_logging_config() -> None:
    """Test if logging config is correctly loaded."""
    config_raw = {
        "root_logger": {"level": "DEBUG"},
        "loggers": {
            "frequenz.sdk.actor": {"level": "INFO"},
            "frequenz.sdk.timeseries": {"level": "ERROR"},
        },
    }
    config = LoggingConfig(
        root_logger=LoggerConfig(level="DEBUG"),
        loggers={
            "frequenz.sdk.actor": LoggerConfig(level="INFO"),
            "frequenz.sdk.timeseries": LoggerConfig(level="ERROR"),
        },
    )

    assert load_config(LoggingConfig, config_raw) == config

    config_raw = {}
    config = LoggingConfig()
    assert load_config(LoggingConfig, config_raw) == config

    config_raw = {"root_logger": {"level": "UNKNOWN"}}
    with pytest.raises(ValidationError):
        load_config(LoggingConfig, config_raw)

    config_raw = {"unknown": {"frequenz.sdk.actor": {"level": "DEBUG"}}}
    with pytest.raises(ValidationError):
        load_config(LoggingConfig, config_raw)


@pytest.fixture
def cleanup_logs() -> Any:
    """Reset logging to default.

    Python doesn't cleanup logging configuration after tests, so we need to do it manually.
    """
    yield

    logging.getLogger("frequenz.sdk.actor").setLevel(logging.NOTSET)
    logging.getLogger("frequenz.sdk.timeseries").setLevel(logging.NOTSET)


async def test_logging_config_updating_actor(
    mocker: MockerFixture,
    cleanup_logs: Any,
) -> None:
    """Test if logging is configured and updated correctly."""
    # Mock method that sets root level logging.
    # Python doesn't cleanup logging configuration after tests.
    # Overriding logging.basicConfig would mess up other tests, so we mock it.
    # This is just for extra safety because changing root logging level in unit tests
    # is not working anyway - python ignores it.
    mocker.patch("frequenz.sdk.config._logging_actor.logging.basicConfig")

    # Mock ConfigManager
    mock_config_manager = mocker.Mock()
    mock_config_manager.config_channel = Broadcast[LoggingConfig | None](name="config")
    mock_config_manager.new_receiver = mocker.AsyncMock(
        return_value=mock_config_manager.config_channel.new_receiver()
    )

    async with LoggingConfigUpdatingActor(
        config_key="logging",
        config_manager=mock_config_manager,
    ) as actor:
        assert logging.getLogger("frequenz.sdk.actor").level == logging.NOTSET
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.NOTSET

        update_logging_spy = mocker.spy(actor, "_update_logging")

        # Send first config
        expected_config = LoggingConfig(
            root_logger=LoggerConfig(level="ERROR"),
            loggers={
                "frequenz.sdk.actor": LoggerConfig(level="DEBUG"),
                "frequenz.sdk.timeseries": LoggerConfig(level="ERROR"),
            },
        )
        await mock_config_manager.config_channel.new_sender().send(expected_config)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(expected_config)
        assert logging.getLogger("frequenz.sdk.actor").level == logging.DEBUG
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.ERROR
        update_logging_spy.reset_mock()

        # Update config
        expected_config = LoggingConfig(
            root_logger=LoggerConfig(level="WARNING"),
            loggers={
                "frequenz.sdk.actor": LoggerConfig(level="INFO"),
            },
        )
        await mock_config_manager.config_channel.new_sender().send(expected_config)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(expected_config)
        assert logging.getLogger("frequenz.sdk.actor").level == logging.INFO
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.NOTSET
        update_logging_spy.reset_mock()

        # Send a None config to make sure actor doesn't crash and configures a default logging
        await mock_config_manager.config_channel.new_sender().send(None)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(LoggingConfig())
        assert (
            actor._current_config == LoggingConfig()  # pylint: disable=protected-access
        )
        update_logging_spy.reset_mock()
