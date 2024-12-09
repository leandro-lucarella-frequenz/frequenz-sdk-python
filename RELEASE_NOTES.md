# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `frequenz.sdk.config`

    * `LoggingConfigUpdater`

        + Renamed to `LoggingConfigUpdatingActor` to follow the actor naming convention.
        + Make all arguments to the constructor keyword-only.

    * `LoggingConfig`

        + The `load()` method was removed. Please use `frequenz.sdk.config.load_config()` instead.
        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.

    * `LoggerConfig` is now a standard `dataclass` instead of a `marshmallow_dataclass`.

## New Features

- `LoggingConfigUpdatingActor`

    * Added a new `name` argument to the constructor to be able to override the actor's name.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
