# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import logging
import pathlib
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Final, Literal, TypeGuard, assert_type, overload

import marshmallow
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
        logging_config_key: str | Sequence[str] | None = "logging",
        name: str | None = None,
        polling_interval: timedelta = timedelta(seconds=5),
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
            logging_config_key: The key to use for the logging configuration. If `None`,
                logging configuration will not be managed.  If a key is provided, the
                manager update the logging configuration whenever the configuration
                changes.
            name: A name to use when creating actors. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
            polling_interval: The interval to poll for changes. Only relevant if
                polling is enabled.
        """
        self.name: Final[str] = str(id(self)) if name is None else name
        """The name of this config manager."""

        self.config_channel: Final[Broadcast[Mapping[str, Any]]] = Broadcast(
            name=f"{self}_config", resend_latest=True
        )
        """The broadcast channel for the configuration."""

        self.config_actor: Final[ConfigManagingActor] = ConfigManagingActor(
            config_paths,
            self.config_channel.new_sender(),
            name=str(self),
            force_polling=force_polling,
            polling_interval=polling_interval,
        )
        """The actor that manages the configuration."""

        # pylint: disable-next=import-outside-toplevel,cyclic-import
        from ._logging_actor import LoggingConfigUpdatingActor

        self.logging_actor: Final[LoggingConfigUpdatingActor | None] = (
            None if logging_config_key is None else LoggingConfigUpdatingActor()
        )

        if auto_start:
            self.config_actor.start()
            if self.logging_actor:
                self.logging_actor.start()

    def __repr__(self) -> str:
        """Return a string representation of this config manager."""
        return (
            f"<{self.__class__.__name__}: "
            f"name={self.name!r}, "
            f"config_channel={self.config_channel!r}, "
            f"logging_actor={self.logging_actor!r}, "
            f"config_actor={self.config_actor!r}>"
        )

    def __str__(self) -> str:
        """Return a string representation of this config manager."""
        return f"{type(self).__name__}[{self.name}]"

    @overload
    def new_receiver(  # pylint: disable=too-many-arguments
        self,
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        skip_none: Literal[False] = False,
        # We need to specify the key here because we have kwargs, so if it is not
        # present is not considered None as the only possible value, as any value can be
        # accepted as part of the kwargs.
        key: None = None,
        base_schema: type[Schema] | None = None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT]: ...

    @overload
    def new_receiver(  # pylint: disable=too-many-arguments
        self,
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        skip_none: Literal[False] = False,
        key: str | Sequence[str],
        base_schema: type[Schema] | None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT | None]: ...

    @overload
    def new_receiver(  # pylint: disable=too-many-arguments
        self,
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        skip_none: Literal[True] = True,
        key: str | Sequence[str],
        base_schema: type[Schema] | None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT]: ...

    # The noqa DOC502 is needed because we raise TimeoutError indirectly.
    # pylint: disable-next=too-many-arguments,too-many-locals
    def new_receiver(  # noqa: DOC502
        self,
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        skip_none: bool = True,
        # This is tricky, because a str is also a Sequence[str], if we would use only
        # Sequence[str], then a regular string would also be accepted and taken as
        # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
        # the allowed types without changing Sequence[str] to something more specific,
        # like list[str] or tuple[str].
        key: str | Sequence[str] | None = None,
        base_schema: type[Schema] | None = None,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT] | Receiver[DataclassT | None]:
        """Create a new receiver for the configuration.

        This method has a lot of features and functionalities to make it easier to
        receive configurations.

        Note:
            If there is a burst of configuration updates, the receiver will only
            receive the last configuration, older configurations will be ignored.

        ### Schema validation

        The raw configuration received as a `Mapping` will be validated and loaded to
        as a `config_class`. The `config_class` class is expected to be
        a [`dataclasses.dataclass`][], which is used to create
        a [`marshmallow.Schema`][] via the [`marshmallow_dataclass.class_schema`][]
        function.

        This means you can customize the schema derived from the configuration
        dataclass using [`marshmallow_dataclass`][] to specify extra validation and
        options via field metadata.

        Configurations that don't pass the validation will be ignored and not sent to
        the receiver, but an error will be logged. Errors other than `ValidationError`
        will not be handled and will be raised.

        Additional arguments can be passed to [`marshmallow.Schema.load`][] using
        the `marshmallow_load_kwargs` keyword arguments.

        ### Skipping superfluous updates

        If `skip_unchanged` is set to `True`, then a configuration that didn't change
        compared to the last one received will be ignored and not sent to the receiver.
        The comparison is done using the *raw* `dict` to determine if the configuration
        has changed.

        If `skip_none` is set to `True`, then a configuration that is `None` will be
        ignored and not sent to the receiver. This is useful for cases where the the
        receiver can't react to `None` configurations, either because it is handled
        externally or because it should just keep the previous configuration.
        This can only be used when `key` is not `None` as when `key` is `None`, the
        configuration can never be `None`.

        ### Filtering

        The configuration can be filtered by a `key`.

        If the `key` is `None`, the receiver will receive the full configuration,
        otherwise only the part of the configuration under the specified key is
        received, or `None` if the key is not found.

        If the key is a sequence of strings, it will be treated as a nested key and the
        receiver will receive the configuration under the nested key. For example
        `["key", "subkey"]` will get only `config["key"]["subkey"]`.

        Example:
            ```python
            # TODO: Add Example
            ```

        Args:
            config_class: The type of the configuration. If provided, the configuration
                will be validated against this type.
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.
            skip_none: Whether to skip sending the configuration if it is `None`. Only
                valid when `key` is not `None`.
            key: The key to filter the configuration. If `None`, the full configuration
                will be received.
            base_schema: An optional class to be used as a base schema for the
                configuration class. This allow using custom fields for example. Will be
                passed to [`marshmallow_dataclass.class_schema`][].
            **marshmallow_load_kwargs: Additional arguments to be passed to
                [`marshmallow.Schema.load`][].

        Returns:
            The receiver for the configuration.
        """
        # All supporting generic function (using DataclassT) need to be nested
        # here. For some reasons mypy has trouble if these functions are
        # global, it consider the DataclassT used by this method and the global
        # functions to be different, leading to very hard to find typing
        # errors.

        @overload
        def _load_config_with_logging(
            config: Mapping[str, Any],
            config_class: type[DataclassT],
            *,
            key: None = None,
            base_schema: type[Schema] | None = None,
            **marshmallow_load_kwargs: Any,
        ) -> DataclassT | _InvalidConfig: ...

        @overload
        def _load_config_with_logging(
            config: Mapping[str, Any],
            config_class: type[DataclassT],
            *,
            key: str | Sequence[str],
            base_schema: type[Schema] | None = None,
            **marshmallow_load_kwargs: Any,
        ) -> DataclassT | None | _InvalidConfig: ...

        def _load_config_with_logging(
            config: Mapping[str, Any],
            config_class: type[DataclassT],
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
                    config_class,
                    config,
                    base_schema=base_schema,
                    **marshmallow_load_kwargs,
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

        def _is_valid_and_not_none(
            config: DataclassT | _InvalidConfig | None,
        ) -> TypeGuard[DataclassT]:
            """Return whether the configuration is valid and not `None`."""
            return config is not _INVALID_CONFIG

        def _is_dataclass(config: DataclassT | None) -> TypeGuard[DataclassT]:
            """Return whether the configuration is a dataclass."""
            return config is not None

        recv_name = f"{self}_receiver" if key is None else f"{self}_receiver_{key}"
        receiver = self.config_channel.new_receiver(name=recv_name, limit=1)

        if skip_unchanged:
            receiver = receiver.filter(WithPrevious(_NotEqualWithLogging(key)))

        match key:
            case None:
                recv_dataclass = receiver.map(
                    lambda config: _load_config_with_logging(
                        config,
                        config_class,
                        # we need to pass it explicitly because of the
                        # variadic keyword arguments, otherwise key
                        # could be included in marshmallow_load_kwargs
                        # with a value different than None.
                        key=None,
                        base_schema=base_schema,
                        **marshmallow_load_kwargs,
                    )
                ).filter(_is_valid_and_not_none)
                assert_type(recv_dataclass, Receiver[DataclassT])
                return recv_dataclass
            case str():
                recv_dataclass_or_none = receiver.map(
                    lambda config: _load_config_with_logging(
                        config,
                        config_class,
                        key=key,
                        base_schema=base_schema,
                        **marshmallow_load_kwargs,
                    )
                ).filter(_is_valid_or_none)
                assert_type(recv_dataclass_or_none, Receiver[DataclassT | None])
                if skip_none:
                    recv_dataclass = recv_dataclass_or_none.filter(_is_dataclass)
                    assert_type(recv_dataclass, Receiver[DataclassT])
                    return recv_dataclass
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
