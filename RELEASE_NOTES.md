# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `frequenz.sdk.config.load_config()` doesn't accept classes decorated with `marshmallow_dataclass.dataclass` anymore. You should use the built-in `dataclasses.dataclass` directly instead, no other changes should be needed, the metadata in the `dataclass` fields will still be used.

## New Features


- `frequenz.sdk.config.load_config()` can now use a base schema to customize even further how data is loaded.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
