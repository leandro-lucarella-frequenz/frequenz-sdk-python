# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `frequenz.sdk.config`

    * `LoggingConfigUpdater`

        + Renamed to `LoggingConfigUpdatingActor` to follow the actor naming convention.
        + Make all arguments to the constructor keyword-only.
        + If the configuration is removed, the actor will now load back the default configuration.

    * `LoggingConfig`

        + The `load()` method was removed. Please use `frequenz.sdk.config.load_config()` instead.
        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

    * `LoggerConfig`

        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

## New Features

- `LoggingConfigUpdatingActor`

    * Added a new `name` argument to the constructor to be able to override the actor's name.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
