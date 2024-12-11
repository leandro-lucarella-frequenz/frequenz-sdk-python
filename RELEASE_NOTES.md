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
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

    * `LoggerConfig`

        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

    * `load_config()`:

         + The `base_schema` argument is now keyword-only.
         + The arguments forwarded to `marshmallow.Schema.load()` now must be passed explicitly via the `marshmallow_load_kwargs` argument, as a `dict`, to improve the type-checking.

## New Features

- `LoggingConfigUpdatingActor`

    * Added a new `name` argument to the constructor to be able to override the actor's name.

## Bug Fixes

- Fix a bug in `BackgroundService` where it won't try to `self.cancel()` and `await self.wait()` if there are no internal tasks. This prevented to properly implement custom stop logic without having to redefine the `stop()` method too.
