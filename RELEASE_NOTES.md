# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* Many tasks, senders and receivers now have proper names for easier debugging.
* The resample log was improved to show more details.
* The `Sample` class now has a nice `__str__` representation.

## Bug Fixes

- Fix a bug in the resampler that could end up with an *IndexError: list index out of range* exception when a new resampler was added while awaiting the existing resampler to finish resampling.
