# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `ConfigManagingActor` now only reacts to `CREATE` and `MODIFY` events. `DELETE` is not supported anymore and are ignored.
- Remove the `event_types` argument from the `ConfigManagingActor` constructor.

## New Features

* Many tasks, senders and receivers now have proper names for easier debugging.
* The resample log was improved to show more details.
* The `Sample` class now has a nice `__str__` representation.

## Bug Fixes

- Fix a bug in the resampler that could end up with an *IndexError: list index out of range* exception when a new resampler was added while awaiting the existing resampler to finish resampling.

- Fix bugs with `ConfigManagingActor`:
  - Raising unhandled exceptions when any file in config directory was deleted.
  - Raising unhandled exception if not all config files exist.
  - Eliminate recursive actor crashes when all config files were missing.