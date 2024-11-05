# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Read and update logging severity from config."""

import logging
from collections.abc import Mapping
from dataclasses import field
from typing import Annotated, Any, Self, cast

import marshmallow
import marshmallow.validate
from frequenz.channels import Receiver
from marshmallow import RAISE
from marshmallow_dataclass import class_schema, dataclass

from frequenz.sdk.actor import Actor

_logger = logging.getLogger(__name__)

LogLevel = Annotated[
    str,
    marshmallow.fields.String(
        validate=marshmallow.validate.OneOf(choices=logging.getLevelNamesMapping())
    ),
]


@dataclass
class LoggerConfig:
    """A configuration for a logger."""

    level: LogLevel = field(
        default="NOTSET",
        metadata={
            "metadata": {
                "description": "Log level for the logger. Uses standard logging levels."
            },
            "required": False,
        },
    )
    """The log level for the logger."""


@dataclass
class LoggingConfig:
    """A configuration for the logging system."""

    root_logger: LoggerConfig = field(
        default_factory=LoggerConfig,
        metadata={
            "metadata": {
                "description": "Default default configuration for all loggers.",
            },
            "required": False,
        },
    )
    """The default log level."""

    loggers: dict[str, LoggerConfig] = field(
        default_factory=dict,
        metadata={
            "metadata": {
                "description": "Configuration for a logger (the key is the logger name)."
            },
            "required": False,
        },
    )
    """The list of loggers configurations."""

    @classmethod
    def load(cls, configs: Mapping[str, Any]) -> Self:  # noqa: DOC502
        """Load and validate configs from a dictionary.

        Args:
            configs: The configuration to validate.

        Returns:
            The configuration if they are valid.

        Raises:
            ValidationError: if the configuration are invalid.
        """
        schema = class_schema(cls)()
        return cast(Self, schema.load(configs, unknown=RAISE))


class LoggingConfigUpdater(Actor):
    """Actor that listens for logging configuration changes and sets them.

    Example:
        `config.toml` file:
        ```toml
        [logging.root_logger]
        level = "INFO"

        [logging.loggers."frequenz.sdk.actor.power_distributing"]
        level = "DEBUG"

        [logging.loggers."frequenz.channels"]
        level = "DEBUG"
        ```

        ```python
        import asyncio
        from collections.abc import Mapping
        from typing import Any

        from frequenz.channels import Broadcast
        from frequenz.sdk.config import LoggingConfigUpdater, ConfigManager
        from frequenz.sdk.actor import run as run_actors

        async def run() -> None:
            config_channel = Broadcast[Mapping[str, Any]](name="config", resend_latest=True)
            actors = [
                ConfigManager(config_paths=["config.toml"], output=config_channel.new_sender()),
                LoggingConfigUpdater(
                    config_recv=config_channel.new_receiver(limit=1)).map(
                        lambda app_config: app_config.get("logging", {}
                    )
                ),
            ]
            await run_actors(*actors)

        asyncio.run(run())
        ```

        Now whenever the `config.toml` file is updated, the logging configuration
        will be updated as well.
    """

    def __init__(
        self,
        config_recv: Receiver[Mapping[str, Any]],
        log_format: str = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)s: %(message)s",
        log_datefmt: str = "%Y-%m-%dT%H:%M:%S%z",
    ):
        """Initialize this instance.

        Args:
            config_recv: The receiver to listen for configuration changes.
            log_format: Use the specified format string in logs.
            log_datefmt: Use the specified date/time format in logs.
        """
        super().__init__()
        self._config_recv = config_recv
        self._format = log_format
        self._datefmt = log_datefmt

        # Setup default configuration.
        # This ensures logging is configured even if actor fails to start or
        # if the configuration cannot be loaded.
        self._current_config: LoggingConfig = LoggingConfig()
        self._update_logging(self._current_config)

    async def _run(self) -> None:
        """Listen for configuration changes and update logging."""
        async for message in self._config_recv:
            try:
                new_config = LoggingConfig.load(message)
            except marshmallow.ValidationError:
                _logger.exception(
                    "Invalid logging configuration received. Skipping config update"
                )
                continue

            if new_config != self._current_config:
                self._update_logging(new_config)

    def _update_logging(self, config: LoggingConfig) -> None:
        """Configure the logging level."""
        # If the logger is not in the new config, set it to NOTSET
        loggers_to_unset = self._current_config.loggers.keys() - config.loggers.keys()
        for logger_id in loggers_to_unset:
            _logger.debug("Unsetting log level for logger '%s'", logger_id)
            logging.getLogger(logger_id).setLevel(logging.NOTSET)

        self._current_config = config
        logging.basicConfig(
            format=self._format,
            level=self._current_config.root_logger.level,
            datefmt=self._datefmt,
        )

        # For each logger in the new config, set the log level
        for logger_id, logger_config in self._current_config.loggers.items():
            _logger.debug(
                "Setting log level for logger '%s' to '%s'",
                logger_id,
                logger_config.level,
            )
            logging.getLogger(logger_id).setLevel(logger_config.level)

        _logger.info("Logging config changed to: %s", self._current_config)
