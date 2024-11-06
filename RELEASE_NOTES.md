# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `ConfigManagingActor` now takes multiple configuration files as input, and the argument was renamed from `config_file` to `config_files`. If you are using this actor, please update your code. For example:

   ```python
   # Old
   actor = ConfigManagingActor(config_file="config.toml")
   # New
   actor = ConfigManagingActor(config_files=["config.toml"])
   ```

* The `MovingWindow` now take all arguments as keyword-only to avoid mistakes.
* The `frequenz-quantities` dependency was bumped to `1.0.0rc3`.

## New Features

- The `ConfigManagingActor` can now take multiple configuration files as input, allowing to override default configurations with custom configurations.
- Implement and standardize logging configuration with the following changes:
   * Add `LoggerConfig` and `LoggingConfig` to standardize logging configuration.
   * Create `LoggingConfigUpdater` to handle runtime config updates.
   * Support individual log level settings for each module.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
