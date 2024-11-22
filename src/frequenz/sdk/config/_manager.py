# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import logging
import pathlib
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Final

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.experimental import WithPrevious
from typing_extensions import override

from ..actor._background_service import BackgroundService
from ._managing_actor import ConfigManagingActor

_logger = logging.getLogger(__name__)


class ConfigManager(BackgroundService):
    """A manager for configuration files.

    This class reads configuration files and sends the configuration to the receivers,
    providing optional configuration key filtering and schema validation.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config_paths: Sequence[pathlib.Path],
        /,
        *,
        force_polling: bool = True,
        name: str | None = None,
        polling_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Initialize this config manager.

        Args:
            config_paths: The paths to the TOML files with the configuration. Order
                matters, as the configuration will be read and updated in the order
                of the paths, so the last path will override the configuration set by
                the previous paths. Dict keys will be merged recursively, but other
                objects (like lists) will be replaced by the value in the last path.
            force_polling: Whether to force file polling to check for changes.
            name: A name to use when creating actors. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
            polling_interval: The interval to poll for changes. Only relevant if
                polling is enabled.
        """
        super().__init__(name=name)

        self.config_channel: Final[Broadcast[Mapping[str, Any]]] = Broadcast(
            name=f"{self}_config", resend_latest=True
        )
        """The broadcast channel for the configuration."""

        self.actor: Final[ConfigManagingActor] = ConfigManagingActor(
            config_paths,
            self.config_channel.new_sender(),
            name=self.name,
            force_polling=force_polling,
            polling_interval=polling_interval,
        )
        """The actor that manages the configuration."""

    @override
    def start(self) -> None:
        """Start this config manager."""
        self.actor.start()

    @property
    @override
    def is_running(self) -> bool:
        """Whether this config manager is running."""
        return self.actor.is_running

    @override
    def cancel(self, msg: str | None = None) -> None:
        """Cancel all running tasks and actors spawned by this config manager.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        self.actor.cancel(msg)

    # We need the noqa because the `BaseExceptionGroup` is raised indirectly.
    @override
    async def wait(self) -> None:  # noqa: DOC502
        """Wait this config manager to finish.

        Wait until all tasks and actors are finished.

        Raises:
            BaseExceptionGroup: If any of the tasks spawned by this service raised an
                exception (`CancelError` is not considered an error and not returned in
                the exception group).
        """
        await self.actor

    @override
    def __repr__(self) -> str:
        """Return a string representation of this config manager."""
        return f"config_channel={self.config_channel!r}, " f"actor={self.actor!r}>"

    def new_receiver(
        self,
        *,
        skip_unchanged: bool = True,
    ) -> Receiver[Mapping[str, Any] | None]:
        """Create a new receiver for the configuration.

        This method has a lot of features and functionalities to make it easier to
        receive configurations, but it also imposes some restrictions on how the
        configurations are received. If you need more control over the configuration
        receiver, you can create a receiver directly using
        [`config_channel.new_receiver()`][frequenz.sdk.config.ConfigManager.config_channel].

        ### Skipping superfluous updates

        If there is a burst of configuration updates, the receiver will only receive the
        last configuration, older configurations will be ignored.

        If `skip_unchanged` is set to `True`, then a configuration that didn't change
        compared to the last one received will be ignored and not sent to the receiver.
        The comparison is done using the *raw* `dict` to determine if the configuration
        has changed.

        Example:
            ```python
            # TODO: Add Example
            ```

        Args:
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.

        Returns:
            The receiver for the configuration.
        """
        receiver = self.config_channel.new_receiver(name=str(self), limit=1)

        if skip_unchanged:
            receiver = receiver.filter(WithPrevious(not_equal_with_logging))

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
