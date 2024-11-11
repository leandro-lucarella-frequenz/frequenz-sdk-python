# Frequenz Python SDK Release Notes

## Summary

This release focus on improving the config management, but also introduces other minor improvements and fixes an important bug.

## Upgrading

- The `ConfigManagingActor` now takes multiple configuration files as input, and the argument was renamed from `config_file` to `config_files`. If you are using this actor, please update your code. For example:

   ```python
   # Old
   actor = ConfigManagingActor(config_file="config.toml")
   # New
   actor = ConfigManagingActor(config_files=["config.toml"])
   ```

- The `MovingWindow` now take all arguments as keyword-only to avoid mistakes.
- The `frequenz-quantities` dependency was bumped to `1.0.0rc3`.
- The `ComponentMetricsRequest` now produces a channel name without the `start_date` if the `start_date` is `None`. If you are somehow relying on the old behavior, please update your code.

## New Features

- The `ConfigManagingActor` can now take multiple configuration files as input, allowing to override default configurations with custom configurations.
- A new `frequenz.sdk.config.load_config()` function is available to load configurations using `marshmallow_dataclass`es with correct type hints.
- Implement and standardize logging configuration with the following changes:
   * Add `LoggerConfig` and `LoggingConfig` to standardize logging configuration.
   * Create `LoggingConfigUpdater` to handle runtime config updates.
   * Support individual log level settings for each module.

## Bug Fixes

- Fixes an issue where PV and EV system bounds were not available to the Power Manager sometimes when requested after startup.
