# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Mixin for reconfigurable classes."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generic,
    Literal,
    Sequence,
    assert_type,
    overload,
)

import marshmallow
from frequenz.channels import Receiver
from marshmallow import Schema

from . import _global
from ._base_schema import BaseConfigSchema
from ._manager import ConfigManager
from ._util import DataclassT


class Reconfigurable(Generic[DataclassT]):
    """A mixin for reconfigurable classes.

    This mixin provides a method to initialize the configuration of a class.  It is
    meant mostly as a guide on how to implement reconfigurable classes.

    TODO: Example in module.
    """

    def __init__(
        self,
        *,
        config_key: str | Sequence[str],
        config_schema: type[DataclassT],
        config_manager: ConfigManager | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize this reconfigurable mixin.

        Args:
            config_key: The key to use to retrieve the configuration from the
                configuration manager.
            config_schema: The schema to use to load the configuration.
            config_manager: The configuration manager to use.  If `None`, the [global
                configuration manager][frequenz.sdk.config.get_config_manager] will be
                used.
            **kwargs: Additional keyword arguments to be passed to the parent class
                constructor. This is only provided to allow this class to be used as
                a mixin alonside other classes that require additional keyword
                arguments.
        """
        self.config_schema: Final[type[DataclassT]] = config_schema
        if not isinstance(config_key, (str, tuple)):
            config_key = tuple(config_key)
        self.config_key: Final[str | tuple[str, ...]] = config_key
        if config_manager is None:
            config_manager = _global.get_config_manager()
        self.config_manager: Final[ConfigManager] = config_manager
        super().__init__(**kwargs)

    @overload
    async def initialize_config(  # noqa: DOC502
        self,
        *,
        skip_unchanged: bool = True,
        skip_none: Literal[True] = True,
        base_schema: type[Schema] | None = BaseConfigSchema,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT]: ...

    @overload
    async def initialize_config(  # noqa: DOC502
        self,
        *,
        skip_unchanged: bool = True,
        skip_none: Literal[False] = False,
        base_schema: type[Schema] | None = BaseConfigSchema,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT | None]: ...

    # The noqa DOC502 is needed because we raise TimeoutError indirectly.
    async def initialize_config(  # noqa: DOC502
        self,
        *,
        skip_unchanged: bool = True,
        skip_none: bool = True,
        base_schema: type[Schema] | None = BaseConfigSchema,
        **marshmallow_load_kwargs: Any,
    ) -> Receiver[DataclassT] | Receiver[DataclassT | None]:
        """Initialize the configuration.

        Args:
            skip_unchanged: Whether to skip unchanged configurations.
            skip_none: Whether to skip sending the configuration if it is `None`. Only
                valid when `key` is not `None`.
            base_schema: The base schema to use for the configuration schema.
            **marshmallow_load_kwargs: Additional arguments to pass to
                `marshmallow.Schema.load`.

        Returns:
            A receiver to get configuration updates,
                [ready][frequenz.channels.Receiver.ready] to receive the first
                configuration.

        Raises:
            asyncio.TimeoutError: If the first configuration can't be received in time.
        """
        if "unknown" not in marshmallow_load_kwargs:
            marshmallow_load_kwargs["unknown"] = marshmallow.EXCLUDE
        if skip_none:
            recv_not_none = await self.config_manager.new_receiver(
                wait_for_first=True,
                skip_unchanged=skip_unchanged,
                skip_none=True,
                key=self.config_key,
                schema=self.config_schema,
                base_schema=base_schema,
                **marshmallow_load_kwargs,
            )
            assert_type(recv_not_none, Receiver[DataclassT])
            return recv_not_none
        recv_none = await self.config_manager.new_receiver(
            wait_for_first=True,
            skip_unchanged=skip_unchanged,
            skip_none=False,
            key=self.config_key,
            schema=self.config_schema,
            base_schema=base_schema,
            **marshmallow_load_kwargs,
        )
        assert_type(recv_none, Receiver[DataclassT | None])
        return recv_none
