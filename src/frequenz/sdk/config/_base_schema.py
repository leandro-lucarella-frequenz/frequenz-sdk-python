# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Base schema for configuration classes."""

import marshmallow
from frequenz.quantities.experimental.marshmallow import QuantitySchema


class BaseConfigSchema(QuantitySchema):
    """A base schema for configuration classes.

    This schema provides validation for quantities and ignores unknown fields by
    default.
    """

    class Meta:
        """Meta options for the schema."""

        unknown = marshmallow.EXCLUDE
