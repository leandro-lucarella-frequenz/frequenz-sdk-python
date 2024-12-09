# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Read and update logging severity from config."""

import logging
from dataclasses import dataclass, field
from typing import Annotated, Sequence, assert_never

import marshmallow
import marshmallow.validate

from ..actor import Actor
from ._manager import ConfigManager

_logger = logging.getLogger(__name__)

LogLevel = Annotated[
    str,
    marshmallow.fields.String(
        validate=marshmallow.validate.OneOf(choices=logging.getLevelNamesMapping())
    ),
]
"""A marshmallow field for validating log levels."""


@dataclass(frozen=True, kw_only=True)
class LoggerConfig:
    """A configuration for a logger."""

    level: LogLevel = field(
        default="NOTSET",
        metadata={
            "metadata": {
                "description": "Log level for the logger. Uses standard logging levels."
            },
        },
    )
    """The log level for the logger."""


@dataclass(frozen=True, kw_only=True)
class LoggingConfig:
    """A configuration for the logging system."""

    root_logger: LoggerConfig = field(
        default_factory=lambda: LoggerConfig(level="INFO"),
        metadata={
            "metadata": {
                "description": "Default default configuration for all loggers.",
            },
        },
    )
    """The default log level."""

    loggers: dict[str, LoggerConfig] = field(
        default_factory=dict,
        metadata={
            "metadata": {
                "description": "Configuration for a logger (the key is the logger name)."
            },
        },
    )
    """The list of loggers configurations."""


class LoggingConfigUpdatingActor(Actor):
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

        from frequenz.sdk.config import LoggingConfigUpdatingActor
        from frequenz.sdk.actor import run as run_actors

        async def run() -> None:
            config_manager: ConfigManager = ...
            await run_actors(LoggingConfigUpdatingActor(config_manager))

        asyncio.run(run())
        ```

        Now whenever the `config.toml` file is updated, the logging configuration
        will be updated as well.
    """

    # pylint: disable-next=too-many-arguments
    def __init__(
        self,
        config_manager: ConfigManager,
        /,
        *,
        config_key: str | Sequence[str] = "logging",
        log_datefmt: str = "%Y-%m-%dT%H:%M:%S%z",
        log_format: str = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)s: %(message)s",
        name: str | None = None,
    ):
        """Initialize this instance.

        Args:
            config_manager: The configuration manager to use.
            config_key: The key to use to retrieve the configuration from the
                configuration manager.  If `None`, the whole configuration will be used.
            log_datefmt: Use the specified date/time format in logs.
            log_format: Use the specified format string in logs.
            name: The name of this actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.

        Note:
            The `log_format` and `log_datefmt` parameters are used in a call to
            `logging.basicConfig()`. If logging has already been configured elsewhere
            in the application (through a previous `basicConfig()` call), then the format
            settings specified here will be ignored.
        """
        self._config_receiver = config_manager.new_receiver(
            config_key, LoggingConfig, base_schema=None
        )

        # Setup default configuration.
        # This ensures logging is configured even if actor fails to start or
        # if the configuration cannot be loaded.
        self._current_config: LoggingConfig = LoggingConfig()

        super().__init__(name=name)

        logging.basicConfig(
            format=log_format,
            datefmt=log_datefmt,
            level=logging.INFO,
        )
        self._update_logging(self._current_config)

    async def _run(self) -> None:
        """Listen for configuration changes and update logging."""
        async for new_config in self._config_receiver:
            match new_config:
                case None:
                    # When we receive None, we want to reset the logging configuration
                    # to the default
                    self._update_logging(LoggingConfig())
                case LoggingConfig():
                    self._update_logging(new_config)
                case Exception():
                    # We ignore errors and just keep the old configuration
                    pass
                case unexpected:
                    assert_never(unexpected)

    def _update_logging(self, config: LoggingConfig) -> None:
        """Configure the logging level."""
        # If the logger is not in the new config, set it to NOTSET
        loggers_to_unset = self._current_config.loggers.keys() - config.loggers.keys()
        for logger_id in loggers_to_unset:
            _logger.debug("Unsetting log level for logger '%s'", logger_id)
            logging.getLogger(logger_id).setLevel(logging.NOTSET)

        self._current_config = config
        _logger.info(
            "Setting root logger level to '%s'", self._current_config.root_logger.level
        )
        logging.getLogger().setLevel(self._current_config.root_logger.level)

        # For each logger in the new config, set the log level
        for logger_id, logger_config in self._current_config.loggers.items():
            _logger.info(
                "Setting log level for logger '%s' to '%s'",
                logger_id,
                logger_config.level,
            )
            logging.getLogger(logger_id).setLevel(logger_config.level)

        _logger.info("Logging config update completed.")
