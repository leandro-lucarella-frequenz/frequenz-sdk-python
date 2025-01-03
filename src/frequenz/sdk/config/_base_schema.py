# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Base schema for configuration classes."""

from frequenz.quantities.experimental.marshmallow import QuantitySchema


class BaseConfigSchema(QuantitySchema):
    """A base schema for configuration classes."""
