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


class InvalidValueForKeyError(ValueError):
    """An error indicating that the value under the specified key is invalid."""

    def __init__(self, msg: str, *, key: str, value: Any) -> None:
        """Initialize this error.

        Args:
            msg: The error message.
            key: The key that has an invalid value.
            value: The actual value that was found that is not a mapping.
        """
        super().__init__(msg)

        self.key: Final[Sequence[str]] = key
        """The key that has an invalid value."""

        self.value: Final[Any] = value
        """The actual value that was found that is not a mapping."""


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
        key: str,
        /,
        *,
        skip_unchanged: bool = True,
    ) -> Receiver[Mapping[str, Any] | InvalidValueForKeyError | None]:
        """Create a new receiver for receiving the configuration for a particular key.

        This method has a lot of features and functionalities to make it easier to
        receive configurations, but it also imposes some restrictions on how the
        configurations are received. If you need more control over the configuration
        receiver, you can create a receiver directly using
        [`config_channel.new_receiver()`][frequenz.sdk.config.ConfigManager.config_channel].

        ### Filtering

        Only the configuration under the `key` will be received by the receiver. If the
        `key` is not found in the configuration, the receiver will receive `None`.

        The value under `key` must be another mapping, otherwise an error
        will be logged and a [`frequenz.sdk.config.InvalidValueForKeyError`][] instance
        will be sent to the receiver.

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
            key: The configuration key to be read by the receiver.
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.

        Returns:
            The receiver for the configuration.
        """
        receiver = self.config_channel.new_receiver(name=f"{self}:{key}", limit=1)

        def _get_key_or_error(
            config: Mapping[str, Any]
        ) -> Mapping[str, Any] | InvalidValueForKeyError | None:
            try:
                return _get_key(config, key)
            except InvalidValueForKeyError as error:
                return error

        key_receiver = receiver.map(_get_key_or_error)

        if skip_unchanged:
            return key_receiver.filter(WithPrevious(_not_equal_with_logging))

        return key_receiver


def _not_equal_with_logging(
    old_value: Mapping[str, Any] | InvalidValueForKeyError | None,
    new_value: Mapping[str, Any] | InvalidValueForKeyError | None,
) -> bool:
    """Return whether the two mappings are not equal, logging if they are the same."""
    if old_value == new_value:
        _logger.info("Configuration has not changed, skipping update")
        return False

    if isinstance(new_value, InvalidValueForKeyError) and not isinstance(
        old_value, InvalidValueForKeyError
    ):
        _logger.error(
            "Configuration for key %r has an invalid value: %r",
            new_value.key,
            new_value.value,
        )
    return True


def _get_key(config: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    """Get the value from the configuration under the specified key.

    Args:
        config: The configuration to get the value from.
        key: The key to get the value for.

    Returns:
        The value under the key, or `None` if the key is not found.

    Raises:
        InvalidValueForKeyError: If the value under the key is not a mapping.
    """
    match config.get(key):
        case None:
            return None
        case Mapping() as value:
            return value
        case invalid_value:
            raise InvalidValueForKeyError(
                f"Value for key {key!r} is not a mapping: {invalid_value!r}",
                key=key,
                value=invalid_value,
            )
