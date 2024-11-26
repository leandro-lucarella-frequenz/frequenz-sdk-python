# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `ConfigManagingActor` now only reacts to `CREATE` and `MODIFY` events. `DELETE` is not supported anymore and are ignored.
- Remove the `event_types` argument from the `ConfigManagingActor` constructor.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Fix a bug in the resampler that could end up with an *IndexError: list index out of range* exception when a new resampler was added while awaiting the existing resampler to finish resampling.

- Fix bugs with `ConfigManagingActor`:
  - Raising unhandled exceptions when any file in config directory was deleted.
  - Raising unhandled exception if not all config files exist.
  - Eliminate recursive actor crashes when all config files were missing.