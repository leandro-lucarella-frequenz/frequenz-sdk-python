# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import pathlib
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Final

from frequenz.channels import Broadcast, Receiver

from ._managing_actor import ConfigManagingActor


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

        if auto_start:
            self.actor.start()

    def __repr__(self) -> str:
        """Return a string representation of this config manager."""
        return (
            f"<{self.__class__.__name__}: "
            f"name={self.name!r}, "
            f"config_channel={self.config_channel!r}, "
            f"actor={self.actor!r}>"
        )

    def __str__(self) -> str:
        """Return a string representation of this config manager."""
        return f"{type(self).__name__}[{self.name}]"

    # The noqa DOC502 is needed because we raise TimeoutError indirectly.
    async def new_receiver(self) -> Receiver[Mapping[str, Any] | None]:  # noqa: DOC502
        """Create a new receiver for the configuration.

        Note:
            If there is a burst of configuration updates, the receiver will only
            receive the last configuration, older configurations will be ignored.

        Example:
            ```python
            # TODO: Add Example
            ```

        Returns:
            The receiver for the configuration.
        """
        recv_name = f"{self}_receiver"
        return self.config_channel.new_receiver(name=recv_name, limit=1)
