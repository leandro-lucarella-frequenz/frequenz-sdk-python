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
from marshmallow import Schema, ValidationError
from typing_extensions import override

from ..actor._background_service import BackgroundService
from ._managing_actor import ConfigManagingActor
from ._util import DataclassT, load_config

_logger = logging.getLogger(__name__)


class InvalidValueForKeyError(ValueError):
    """An error indicating that the value under the specified key is invalid."""

    def __init__(self, msg: str, *, key: Sequence[str], value: Any) -> None:
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

    def new_receiver(  # pylint: disable=too-many-arguments
        self,
        # This is tricky, because a str is also a Sequence[str], if we would use only
        # Sequence[str], then a regular string would also be accepted and taken as
        # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
        # the allowed types without changing Sequence[str] to something more specific,
        # like list[str] or tuple[str] (but both have their own problems).
        key: str | Sequence[str],
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        base_schema: type[Schema] | None = None,
        marshmallow_load_kwargs: dict[str, Any] | None = None,
    ) -> Receiver[DataclassT | Exception | None]:
        """Create a new receiver for receiving the configuration for a particular key.

        This method has a lot of features and functionalities to make it easier to
        receive configurations, but it also imposes some restrictions on how the
        configurations are received. If you need more control over the configuration
        receiver, you can create a receiver directly using
        [`config_channel.new_receiver()`][frequenz.sdk.config.ConfigManager.config_channel].

        ### Filtering

        Only the configuration under the `key` will be received by the receiver. If the
        `key` is not found in the configuration, the receiver will receive `None`.

        If the key is a sequence of strings, it will be treated as a nested key and the
        receiver will receive the configuration under the nested key. For example
        `["key", "subkey"]` will get only `config["key"]["subkey"]`.

        The value under `key` must be another mapping, otherwise an error
        will be logged and a [`frequenz.sdk.config.InvalidValueForKeyError`][] instance
        will be sent to the receiver.

        ### Schema validation

        The raw configuration received as a `Mapping` will be validated and loaded to
        as a `config_class`. The `config_class` class is expected to be
        a [`dataclasses.dataclass`][], which is used to create
        a [`marshmallow.Schema`][] via the [`marshmallow_dataclass.class_schema`][]
        function.

        This means you can customize the schema derived from the configuration
        dataclass using [`marshmallow_dataclass`][] to specify extra validation and
        options via field metadata.

        Additional arguments can be passed to [`marshmallow.Schema.load`][] using
        the `marshmallow_load_kwargs` keyword arguments.

        ### Skipping superfluous updates

        If there is a burst of configuration updates, the receiver will only receive the
        last configuration, older configurations will be ignored.

        If `skip_unchanged` is set to `True`, then a configuration that didn't change
        compared to the last one received will be ignored and not sent to the receiver.
        The comparison is done using the *raw* `dict` to determine if the configuration
        has changed.

        ### Error handling

        The value under `key` must be another mapping, otherwise an error
        will be logged and a [`frequenz.sdk.config.InvalidValueForKeyError`][] instance
        will be sent to the receiver.

        Configurations that don't pass the validation will be logged as an error and
        the [`ValidationError`][marshmallow.ValidationError] sent to the receiver.

        Any other unexpected error raised during the configuration loading will be
        logged as an error and the error instance sent to the receiver.

        Example:
            ```python
            # TODO: Add Example
            ```

        Args:
            key: The configuration key to be read by the receiver. If a sequence of
                strings is used, it is used as a sub-key.
            config_class: The class object to use to instantiate a configuration. The
                configuration will be validated against this type too using
                [`marshmallow_dataclass`][].
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.
            base_schema: An optional class to be used as a base schema for the
                configuration class. This allow using custom fields for example. Will be
                passed to [`marshmallow_dataclass.class_schema`][].
            marshmallow_load_kwargs: Additional arguments to be passed to
                [`marshmallow.Schema.load`][].

        Returns:
            The receiver for the configuration.
        """
        receiver = self.config_channel.new_receiver(name=f"{self}:{key}", limit=1).map(
            lambda config: _load_config_with_logging_and_errors(
                config,
                config_class,
                key=key,
                base_schema=base_schema,
                marshmallow_load_kwargs=marshmallow_load_kwargs,
            )
        )

        if skip_unchanged:
            # For some reason the type argument for WithPrevious is not inferred
            # correctly, so we need to specify it explicitly.
            return receiver.filter(
                WithPrevious[DataclassT | Exception | None](
                    lambda old, new: _not_equal_with_logging(
                        key=key, old_value=old, new_value=new
                    )
                )
            )

        return receiver


def _not_equal_with_logging(
    *,
    key: str | Sequence[str],
    old_value: DataclassT | Exception | None,
    new_value: DataclassT | Exception | None,
) -> bool:
    """Return whether the two mappings are not equal, logging if they are the same."""
    if old_value == new_value:
        _logger.info("Configuration has not changed for key %r, skipping update.", key)
        return False

    if isinstance(new_value, InvalidValueForKeyError) and not isinstance(
        old_value, InvalidValueForKeyError
    ):
        subkey_str = ""
        if key != new_value.key:
            subkey_str = f"When looking for sub-key {key!r}: "
        _logger.error(
            "%sConfiguration for key %r has an invalid value: %r",
            subkey_str,
            new_value.key,
            new_value.value,
        )
    return True


def _load_config_with_logging_and_errors(
    config: Mapping[str, Any],
    config_class: type[DataclassT],
    *,
    key: str | Sequence[str],
    base_schema: type[Schema] | None = None,
    marshmallow_load_kwargs: dict[str, Any] | None = None,
) -> DataclassT | Exception | None:
    """Load the configuration for the specified key, logging errors and returning them."""
    try:
        sub_config = _get_key(config, key)
        if sub_config is None:
            _logger.debug("Configuration key %r not found, sending None", key)
            return None

        loaded_config = load_config(
            config_class,
            sub_config,
            base_schema=base_schema,
            marshmallow_load_kwargs=marshmallow_load_kwargs,
        )
        _logger.debug("Received new configuration: %s", loaded_config)
        return loaded_config
    except InvalidValueForKeyError as error:
        if len(key) > 1 and key != error.key:
            _logger.error("Error when looking for sub-key %r: %s", key, error)
        else:
            _logger.error(str(error))
        return error
    except ValidationError as error:
        _logger.error("The configuration for key %r is invalid: %s", key, error)
        return error
    except Exception as error:  # pylint: disable=broad-except
        _logger.exception(
            "An unexpected error occurred while loading the configuration for key %r: %s",
            key,
            error,
        )
        return error


def _get_key(
    config: Mapping[str, Any],
    # This is tricky, because a str is also a Sequence[str], if we would use only
    # Sequence[str], then a regular string would also be accepted and taken as
    # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
    # the allowed types without changing Sequence[str] to something more specific,
    # like list[str] or tuple[str].
    key: str | Sequence[str],
) -> Mapping[str, Any] | None:
    """Get the value from the configuration under the specified key.

    Args:
        config: The configuration to get the value from.
        key: The key to get the value for.

    Returns:
        The value under the key, or `None` if the key is not found.

    Raises:
        InvalidValueForKeyError: If the value under the key is not a mapping.
    """
    # We first normalize to a Sequence[str] to make it easier to work with.
    if isinstance(key, str):
        key = (key,)
    value = config
    current_path = []
    for subkey in key:
        current_path.append(subkey)
        if value is None:
            return None
        match value.get(subkey):
            case None:
                return None
            case Mapping() as new_value:
                value = new_value
            case invalid_value:
                raise InvalidValueForKeyError(
                    f"Value for key {current_path!r} is not a mapping: {invalid_value!r}",
                    key=current_path,
                    value=invalid_value,
                )
        value = new_value
    return value
