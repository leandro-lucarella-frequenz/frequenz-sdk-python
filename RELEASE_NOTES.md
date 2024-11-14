# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `frequenz.sdk.config.load_config()` doesn't accept classes decorated with `marshmallow_dataclass.dataclass` anymore. You should use the built-in `dataclasses.dataclass` directly instead, no other changes should be needed, the metadata in the `dataclass` fields will still be used.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
