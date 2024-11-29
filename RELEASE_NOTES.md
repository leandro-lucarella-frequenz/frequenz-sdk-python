# Frequenz Python SDK Release Notes

## Summary

This is a minor release with just a few bug fixes but also one breaking change in the `ConfigManagingActor`.

## Upgrading

- The `ConfigManagingActor` now only reacts to `CREATE` and `MODIFY` events. `DELETE` is not supported anymore and are ignored.
- Remove the `event_types` argument from the `ConfigManagingActor` constructor.

## Bug Fixes

- Fix bugs with `ConfigManagingActor`:
  - Raising unhandled exceptions when any file in config directory was deleted.
  - Raising unhandled exception if not all config files exist.
  - Eliminate recursive actor crashes when all config files were missing.
