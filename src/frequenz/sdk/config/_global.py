# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Global config manager."""

import asyncio
import logging
import pathlib
from collections.abc import Sequence
from datetime import timedelta

from ._manager import ConfigManager

_logger = logging.getLogger(__name__)

# pylint: disable=global-statement
_CONFIG_MANAGER: ConfigManager | None = None
"""Global instance of the ConfigManagingActor.

This is the only instance of the ConfigManagingActor that should be used in the
entire application. It is created lazily on the first access and should be
accessed via the `get_config_manager` function.
"""


def initialize_config_manager(  # pylint: disable=too-many-arguments
    config_paths: Sequence[pathlib.Path],
    /,
    *,
    force_polling: bool = True,
    name: str = "global",
    polling_interval: timedelta = timedelta(seconds=5),
    wait_for_first_timeout: timedelta = timedelta(seconds=5),
) -> ConfigManager:
    """Initialize the singleton instance of the ConfigManagingActor.

    Args:
        config_paths: Paths to the TOML configuration files.
        force_polling: Whether to force file polling to check for changes.
        name: The name of the config manager.
        polling_interval: The interval to poll for changes. Only relevant if
            polling is enabled.
        wait_for_first_timeout: The timeout to use when waiting for the first
            configuration in
            [`new_receiver`][frequenz.sdk.config.ConfigManager.new_receiver] if
            `wait_for_first` is `True`.

    Returns:
        The global instance of the ConfigManagingActor.

    Raises:
        RuntimeError: If the config manager is already initialized.
    """
    _logger.info(
        "Initializing config manager %s for %s with force_polling=%s, "
        "polling_interval=%s, wait_for_first_timeout=%s",
        name,
        config_paths,
        force_polling,
        polling_interval,
        wait_for_first_timeout,
    )

    global _CONFIG_MANAGER
    if _CONFIG_MANAGER is not None:
        raise RuntimeError("Config already initialized")

    _CONFIG_MANAGER = ConfigManager(
        config_paths,
        name=name,
        force_polling=force_polling,
        polling_interval=polling_interval,
        wait_for_first_timeout=wait_for_first_timeout,
        auto_start=True,
    )

    return _CONFIG_MANAGER


async def shutdown_config_manager(
    *,
    msg: str = "Config manager is shutting down",
    timeout: timedelta | None = timedelta(seconds=5),
) -> None:
    """Shutdown the global config manager.

    This will stop the config manager and release any resources it holds.

    Note:
        The config manager must be
        [initialized][frequenz.sdk.config.initialize_config] before calling this
        function.

    Args:
        msg: The message to be passed to the tasks being cancelled.
        timeout: The maximum time to wait for the config manager to stop. If `None`,
            the method will only cancel the config manager actor and return immediately
            without awaiting at all (stopping might continue in the background). If the
            time is exceeded, an error will be logged.

    Raises:
        RuntimeError: If the config manager is not initialized.
    """
    _logger.info("Shutting down config manager (timeout=%s)...", timeout)

    global _CONFIG_MANAGER
    if _CONFIG_MANAGER is None:
        raise RuntimeError("Config not initialized")

    if timeout is None:
        _CONFIG_MANAGER.actor.cancel(msg)
        _logger.info(
            "Config manager cancelled, stopping might continue in the background."
        )
    else:
        try:
            async with asyncio.timeout(timeout.total_seconds()):
                await _CONFIG_MANAGER.actor.stop(msg)
                _logger.info("Config manager stopped.")
        except asyncio.TimeoutError:
            _logger.warning(
                "Config manager did not stop within %s seconds, it might continue "
                "stopping in the background",
                timeout,
            )

    _CONFIG_MANAGER = None


def get_config_manager() -> ConfigManager:
    """Return the global config manager.

    Note:
        The config manager must be
        [initialized][frequenz.sdk.config.initialize_config] before calling this
        function.

    Returns:
        The global instance of the config manager.

    Raises:
        RuntimeError: If the config manager is not initialized.
    """
    if _CONFIG_MANAGER is None:
        raise RuntimeError("Config not initialized")
    return _CONFIG_MANAGER
