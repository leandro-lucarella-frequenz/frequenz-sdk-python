# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import asyncio
import logging
import pathlib
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Final

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.experimental import WithPrevious

from ._managing_actor import ConfigManagingActor

_logger = logging.getLogger(__name__)


class ConfigManager:
    """A manager for configuration files.

    This class reads configuration files and sends the configuration to the receivers,
    providing optional configuration key filtering and schema validation.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config_paths: Sequence[pathlib.Path],
        /,
        *,
        auto_start: bool = True,
        force_polling: bool = True,
        name: str | None = None,
        polling_interval: timedelta = timedelta(seconds=5),
        wait_for_first_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        """Initialize this config manager.

        Args:
            config_paths: The paths to the TOML files with the configuration. Order
                matters, as the configuration will be read and updated in the order
                of the paths, so the last path will override the configuration set by
                the previous paths. Dict keys will be merged recursively, but other
                objects (like lists) will be replaced by the value in the last path.
            auto_start: Whether to start the actor automatically. If `False`, the actor
                will need to be started manually by calling `start()` on the actor.
            force_polling: Whether to force file polling to check for changes.
            name: A name to use when creating actors. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
            polling_interval: The interval to poll for changes. Only relevant if
                polling is enabled.
            wait_for_first_timeout: The timeout to use when waiting for the first
                configuration in
                [`new_receiver`][frequenz.sdk.config.ConfigManager.new_receiver] if
                `wait_for_first` is `True`.
        """
        self.name: Final[str] = str(id(self)) if name is None else name
        """The name of this config manager."""

        self.config_channel: Final[Broadcast[Mapping[str, Any]]] = Broadcast(
            name=f"{self}_config", resend_latest=True
        )
        """The broadcast channel for the configuration."""

        self.actor: Final[ConfigManagingActor] = ConfigManagingActor(
            config_paths,
            self.config_channel.new_sender(),
            name=str(self),
            force_polling=force_polling,
            polling_interval=polling_interval,
        )
        """The actor that manages the configuration."""

        self.wait_for_first_timeout: timedelta = wait_for_first_timeout
        """The timeout to use when waiting for the first configuration.

        When passing `wait_for_first` as `True` to
        [`new_receiver`][frequenz.sdk.config.ConfigManager.new_receiver], this timeout
        will be used to wait for the first configuration to be received.
        """

        if auto_start:
            self.actor.start()

    def __repr__(self) -> str:
        """Return a string representation of this config manager."""
        return (
            f"<{self.__class__.__name__}: "
            f"name={self.name!r}, "
            f"wait_for_first_timeout={self.wait_for_first_timeout!r}, "
            f"config_channel={self.config_channel!r}, "
            f"actor={self.actor!r}>"
        )

    def __str__(self) -> str:
        """Return a string representation of this config manager."""
        return f"{type(self).__name__}[{self.name}]"

    # The noqa DOC502 is needed because we raise TimeoutError indirectly.
    async def new_receiver(  # pylint: disable=too-many-arguments # noqa: DOC502
        self,
        *,
        wait_for_first: bool = False,
        skip_unchanged: bool = True,
    ) -> Receiver[Mapping[str, Any] | None]:
        """Create a new receiver for the configuration.

        This method has a lot of features and functionalities to make it easier to
        receive configurations.

        Note:
            If there is a burst of configuration updates, the receiver will only
            receive the last configuration, older configurations will be ignored.

        ### Skipping superfluous updates

        If `skip_unchanged` is set to `True`, then a configuration that didn't change
        compared to the last one received will be ignored and not sent to the receiver.
        The comparison is done using the *raw* `dict` to determine if the configuration
        has changed.

        ### Waiting for the first configuration

        If `wait_for_first` is `True`, the receiver will wait for the first
        configuration to be received before returning the receiver. If the
        configuration can't be received in time, a timeout error will be raised.

        If the configuration is received successfully, the first configuration can be
        simply retrieved by calling [`consume()`][frequenz.channels.Receiver.consume] on
        the receiver without blocking.

        Example:
            ```python
            # TODO: Add Example
            ```

        Args:
            wait_for_first: Whether to wait for the first configuration to be received
                before returning the receiver. If the configuration can't be received
                for
                [`wait_for_first_timeout`][frequenz.sdk.config.ConfigManager.wait_for_first_timeout]
                time, a timeout error will be raised. If receiving was successful, the
                first configuration can be simply retrieved by calling
                [`consume()`][frequenz.channels.Receiver.consume] on the receiver.
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.

        Returns:
            The receiver for the configuration.

        Raises:
            asyncio.TimeoutError: If `wait_for_first` is `True` and the first
                configuration can't be received in time.
        """
        recv_name = f"{self}_receiver"
        receiver = self.config_channel.new_receiver(name=recv_name, limit=1)

        if skip_unchanged:
            receiver = receiver.filter(WithPrevious(not_equal_with_logging))

        if wait_for_first:
            async with asyncio.timeout(self.wait_for_first_timeout.total_seconds()):
                await receiver.ready()

        return receiver


def not_equal_with_logging(
    old_config: Mapping[str, Any], new_config: Mapping[str, Any]
) -> bool:
    """Return whether the two mappings are not equal, logging if they are the same."""
    if old_config == new_config:
        _logger.info("Configuration has not changed, skipping update")
        _logger.debug("Old configuration being kept: %r", old_config)
        return False
    return True
