# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import asyncio
import logging
import pathlib
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Final, TypeGuard, assert_type, overload

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.experimental import WithPrevious
from marshmallow import Schema, ValidationError

from ._managing_actor import ConfigManagingActor
from ._util import DataclassT, load_config

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

    @overload
    async def new_receiver(
        self,
        *,
        wait_for_first: bool = True,
        skip_unchanged: bool = True,
    ) -> Receiver[Mapping[str, Any]]: ...

    @overload
    async def new_receiver(  # pylint: disable=too-many-arguments
        self,
        *,
        wait_for_first: bool = True,
        skip_unchanged: bool = True,
        # We need to specify the key here because we have kwargs, so if it is not
        # present is not considered None as the only possible value, as any value can be
        # accepted as part of the kwargs.
        key: None = None,
        schema: type[DataclassT],
        base_schema: type[Schema] | None = None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT]: ...

    @overload
    async def new_receiver(
        self,
        *,
        wait_for_first: bool = True,
        skip_unchanged: bool = True,
        key: str | Sequence[str],
    ) -> Receiver[Mapping[str, Any] | None]: ...

    @overload
    async def new_receiver(  # pylint: disable=too-many-arguments
        self,
        *,
        wait_for_first: bool = True,
        skip_unchanged: bool = True,
        key: str | Sequence[str],
        schema: type[DataclassT],
        base_schema: type[Schema] | None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT | None]: ...

    # The noqa DOC502 is needed because we raise TimeoutError indirectly.
    async def new_receiver(  # pylint: disable=too-many-arguments # noqa: DOC502
        self,
        *,
        wait_for_first: bool = False,
        skip_unchanged: bool = True,
        # This is tricky, because a str is also a Sequence[str], if we would use only
        # Sequence[str], then a regular string would also be accepted and taken as
        # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
        # the allowed types without changing Sequence[str] to something more specific,
        # like list[str] or tuple[str].
        key: str | Sequence[str] | None = None,
        schema: type[DataclassT] | None = None,
        base_schema: type[Schema] | None = None,
        **marshmallow_load_kwargs: Any,
    ) -> (
        Receiver[Mapping[str, Any]]
        | Receiver[Mapping[str, Any] | None]
        | Receiver[DataclassT]
        | Receiver[DataclassT | None]
    ):
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

        ### Filtering

        The configuration can be filtered by a `key`.

        If the `key` is `None`, the receiver will receive the full configuration,
        otherwise only the part of the configuration under the specified key is
        received, or `None` if the key is not found.

        If the key is a sequence of strings, it will be treated as a nested key and the
        receiver will receive the configuration under the nested key. For example
        `["key", "subkey"]` will get only `config["key"]["subkey"]`.

        ### Schema validation

        The configuration is received as a dictionary unless a `schema` is provided. In
        this case, the configuration will be validated against the schema and received
        as an instance of the configuration class.

        The configuration `schema` class is expected to be
        a [`dataclasses.dataclass`][], which is used to create
        a [`marshmallow.Schema`][] schema to validate the configuration dictionary.

        To customize the schema derived from the configuration dataclass, you can
        use [`marshmallow_dataclass.dataclass`][] to specify extra metadata.

        Configurations that don't pass the validation will be ignored and not sent to
        the receiver, but an error will be logged. Errors other than `ValidationError`
        will not be handled and will be raised.

        Additional arguments can be passed to [`marshmallow.Schema.load`][] using keyword
        arguments.

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
            key: The key to filter the configuration. If `None`, the full configuration
                will be received.
            schema: The type of the configuration. If provided, the configuration
                will be validated against this type.
            base_schema: An optional class to be used as a base schema for the
                configuration class. This allow using custom fields for example. Will be
                passed to [`marshmallow_dataclass.class_schema`][].
            **marshmallow_load_kwargs: Additional arguments to be passed to
                [`marshmallow.Schema.load`][].

        Returns:
            The receiver for the configuration.

        Raises:
            asyncio.TimeoutError: If `wait_for_first` is `True` and the first
                configuration can't be received in time.
        """
        # All supporting generic function (using DataclassT) need to be nested
        # here. For some reasons mypy has trouble if these functions are
        # global, it consider the DataclassT used by this method and the global
        # functions to be different, leading to very hard to find typing
        # errors.

        @overload
        def _load_config_with_logging(
            config: Mapping[str, Any],
            schema: type[DataclassT],
            *,
            key: None = None,
            base_schema: type[Schema] | None = None,
            **marshmallow_load_kwargs: Any,
        ) -> DataclassT | _InvalidConfig: ...

        @overload
        def _load_config_with_logging(
            config: Mapping[str, Any],
            schema: type[DataclassT],
            *,
            key: str | Sequence[str],
            base_schema: type[Schema] | None = None,
            **marshmallow_load_kwargs: Any,
        ) -> DataclassT | None | _InvalidConfig: ...

        def _load_config_with_logging(
            config: Mapping[str, Any],
            schema: type[DataclassT],
            *,
            key: str | Sequence[str] | None = None,
            base_schema: type[Schema] | None = None,
            **marshmallow_load_kwargs: Any,
        ) -> DataclassT | None | _InvalidConfig:
            """Try to load a configuration and log any validation errors."""
            if key is not None:
                maybe_config = _get_key(config, key)
                if maybe_config is None:
                    _logger.debug(
                        "Configuration key %s not found, sending None: config=%r",
                        key,
                        config,
                    )
                    return None
                config = maybe_config

            try:
                return load_config(
                    schema, config, base_schema=base_schema, **marshmallow_load_kwargs
                )
            except ValidationError as err:
                key_str = ""
                if key:
                    key_str = f" for key '{key}'"
                _logger.error(
                    "The configuration%s is invalid, the configuration update will be skipped: %s",
                    key_str,
                    err,
                )
                return _INVALID_CONFIG

        def _is_valid_or_none(
            config: DataclassT | _InvalidConfig | None,
        ) -> TypeGuard[DataclassT | None]:
            """Return whether the configuration is valid or `None`."""
            return config is not _INVALID_CONFIG

        def _is_valid(
            config: DataclassT | _InvalidConfig,
        ) -> TypeGuard[DataclassT]:
            """Return whether the configuration is valid and not `None`."""
            return config is not _INVALID_CONFIG

        recv_name = f"{self}_receiver" if key is None else f"{self}_receiver_{key}"
        receiver = self.config_channel.new_receiver(name=recv_name, limit=1)

        if skip_unchanged:
            receiver = receiver.filter(WithPrevious(_NotEqualWithLogging(key)))

        if wait_for_first:
            async with asyncio.timeout(self.wait_for_first_timeout.total_seconds()):
                await receiver.ready()

        match (key, schema):
            case (None, None):
                assert_type(receiver, Receiver[Mapping[str, Any]])
                return receiver
            case (None, type()):
                recv_dataclass = receiver.map(
                    lambda config: _load_config_with_logging(
                        config,
                        schema,
                        # we need to pass it explicitly because of the
                        # variadic keyword arguments, otherwise key
                        # could be included in marshmallow_load_kwargs
                        # with a value different than None.
                        key=None,
                        base_schema=base_schema,
                        **marshmallow_load_kwargs,
                    )
                ).filter(_is_valid)
                assert_type(recv_dataclass, Receiver[DataclassT])
                return recv_dataclass
            case (str(), None):
                recv_map_or_none = receiver.map(lambda config: _get_key(config, key))
                assert_type(recv_map_or_none, Receiver[Mapping[str, Any] | None])
                return recv_map_or_none
            case (str(), type()):
                recv_dataclass_or_none = receiver.map(
                    lambda config: _load_config_with_logging(
                        config,
                        schema,
                        key=key,
                        base_schema=base_schema,
                        **marshmallow_load_kwargs,
                    )
                ).filter(_is_valid_or_none)
                assert_type(recv_dataclass_or_none, Receiver[DataclassT | None])
                return recv_dataclass_or_none
            case unexpected:
                # We can't use `assert_never` here because `mypy` is
                # having trouble
                # narrowing the types of a tuple.
                assert False, f"Unexpected match: {unexpected}"


class _NotEqualWithLogging:
    """A predicate that returns whether the two mappings are not equal.

    If the mappings are equal, a logging message will be emitted indicating that the
    configuration has not changed for the specified key.
    """

    def __init__(self, key: str | Sequence[str] | None) -> None:
        """Initialize this instance.

        Args:
            key: The key to use in the logging message.
        """
        self._key = key

    def __call__(
        self, old_config: Mapping[str, Any] | None, new_config: Mapping[str, Any] | None
    ) -> bool:
        """Return whether the two mappings are not equal, logging if they are the same."""
        key = self._key
        if key is None:
            has_changed = new_config != old_config
        else:
            match (new_config, old_config):
                case (None, None):
                    has_changed = False
                case (None, Mapping()):
                    has_changed = _get_key(old_config, key) is not None
                case (Mapping(), None):
                    has_changed = _get_key(new_config, key) is not None
                case (Mapping(), Mapping()):
                    has_changed = _get_key(new_config, key) != _get_key(old_config, key)
                case unexpected:
                    # We can't use `assert_never` here because `mypy` is having trouble
                    # narrowing the types of a tuple. See for example:
                    # https://github.com/python/mypy/issues/16722
                    # https://github.com/python/mypy/issues/16650
                    # https://github.com/python/mypy/issues/14833
                    # assert_never(unexpected)
                    assert False, f"Unexpected match: {unexpected}"

        if not has_changed:
            key_str = f" for key '{key}'" if key else ""
            _logger.info("Configuration%s has not changed, skipping update", key_str)
            _logger.debug("Old configuration%s being kept: %r", key_str, old_config)

        return has_changed


def _get_key(
    config: Mapping[str, Any],
    # This is tricky, because a str is also a Sequence[str], if we would use only
    # Sequence[str], then a regular string would also be accepted and taken as
    # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
    # the allowed types without changing Sequence[str] to something more specific,
    # like list[str] or tuple[str].
    key: str | Sequence[str] | None,
) -> Mapping[str, Any] | None:
    """Get the value from the configuration under the specified key."""
    if key is None:
        return config
    # Order here is very important too, str() needs to come first, otherwise a regular
    # will also match the Sequence[str] case.
    # TODO: write tests to validate this works correctly.
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
            case _:
                subkey_str = ""
                if len(key) > 1:
                    subkey_str = f" when looking for sub-key {key!r}"
                _logger.error(
                    "Found key %r%s but it's not a mapping, returning None: config=%r",
                    current_path[0] if len(current_path) == 1 else current_path,
                    subkey_str,
                    config,
                )
                return None
        value = new_value
    return value


class _InvalidConfig:
    """A sentinel to represent an invalid configuration."""


_INVALID_CONFIG = _InvalidConfig()
"""A sentinel singleton instance to represent an invalid configuration."""
