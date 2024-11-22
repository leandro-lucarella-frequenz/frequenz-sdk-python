# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Configuration management.

Example: App configuring the global config manager.
    ```python
    import asyncio
    import dataclasses
    import sys

    import marshmallow

    from frequenz.channels import select, selected_from
    from frequenz.sdk.actor import Actor
    from frequenz.sdk.config import (
        initialize_config,
        config_manager,
        LoggingConfigUpdatingActor,
        ConfigManager,
    )

    @dataclasses.dataclass
    class ActorConfig:
        name: str

    class MyActor(Actor):
        def __init__(self, config: ActorConfig) -> None:
            self._config = config
            super().__init__()

        async def _run(self) -> None:
            receiver = ...
            config_receiver = await config_manager().new_receiver(schema=ActorConfig)

            async for selected in select(receiver, config_receiver):
                if selected_from(selected, receiver):
                    ...
                elif selected_from(selected, config_receiver):
                    self._config = selected.message
                    # Restart whatever is needed after a config update


    @dataclasses.dataclass
    class AppConfig:
        positive_int: int = dataclasses.field(
            default=42,
            metadata={"validate": marshmallow.validate.Range(min=0)},
        )
        my_actor: ActorConfig | None = None
        logging: LoggingConfig = LoggingConfig()

    async def main() -> None:
        config_manager = initialize_config_manager(["config.toml"])
        try:
            # Receive the first configuration
            initial_config = await config_manager.new_receiver(schema=AppConfig,
                                                                  wait_for_first=True)
        except asyncio.TimeoutError:
            print("No configuration received in time")
            sys.exit(1)

        actor = MyActor(ActorConfig(name=initial_config.my_actor))
        actor.start()
        await actor
    ```
"""

from ._logging_actor import LoggerConfig, LoggingConfig, LoggingConfigUpdatingActor
from ._manager import ConfigManager
from ._managing_actor import ConfigManagingActor
from ._util import load_config

__all__ = [
    "ConfigManager",
    "ConfigManagingActor",
    "LoggerConfig",
    "LoggingConfig",
    "LoggingConfigUpdatingActor",
    "load_config",
]
