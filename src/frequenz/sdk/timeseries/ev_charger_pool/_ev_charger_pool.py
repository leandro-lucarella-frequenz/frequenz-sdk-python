# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""


import uuid
from collections import abc

from ..._internal._channels import ReceiverFetcher
from .._base_types import SystemBounds
from .._quantities import Current, Power
from ..formula_engine import FormulaEngine, FormulaEngine3Phase
from ..formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
    FormulaGeneratorConfig,
)
from ._ev_charger_pool_reference_store import EVChargerPoolReferenceStore


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


class EVChargerPool:
    """An interface for interaction with pools of EV Chargers.

    !!! note
        `EVChargerPool` instances are not meant to be created directly by users. Use the
        [`microgrid.ev_charger_pool`][frequenz.sdk.microgrid.ev_charger_pool] method for
        creating `EVChargerPool` instances.

    Provides:
      - Aggregate [`power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.power]
        and 3-phase
        [`current`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.current]
        measurements of the EV Chargers in the pool.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ev_charger_pool_ref: EVChargerPoolReferenceStore,
        name: str | None,
        priority: int,
    ) -> None:
        """Create an `EVChargerPool` instance.

        !!! note
            `EVChargerPool` instances are not meant to be created directly by users. Use
            the [`microgrid.ev_charger_pool`][frequenz.sdk.microgrid.ev_charger_pool]
            method for creating `EVChargerPool` instances.

        Args:
            ev_charger_pool_ref: The EV charger pool reference store instance.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            priority: The priority of the actor using this wrapper.
        """
        self._ev_charger_pool = ev_charger_pool_ref
        unique_id = uuid.uuid4()
        self._source_id = unique_id if name is None else f"{name}-{unique_id}"
        self._priority = priority

    @property
    def component_ids(self) -> abc.Set[int]:
        """Return component IDs of all EV Chargers managed by this EVChargerPool.

        Returns:
            Set of managed component IDs.
        """
        return self._ev_charger_pool.component_ids

    @property
    def current(self) -> FormulaEngine3Phase[Current]:
        """Fetch the total current for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate EV Charger current is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total current of all EV
                Chargers.
        """
        engine = (
            self._ev_charger_pool.formula_pool.from_3_phase_current_formula_generator(
                "ev_charger_total_current",
                EVChargerCurrentFormula,
                FormulaGeneratorConfig(
                    component_ids=self._ev_charger_pool.component_ids
                ),
            )
        )
        assert isinstance(engine, FormulaEngine3Phase)
        return engine

    @property
    def power(self) -> FormulaEngine[Power]:
        """Fetch the total power for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate EV Charger power is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total power of all EV
                Chargers.
        """
        engine = self._ev_charger_pool.formula_pool.from_power_formula_generator(
            "ev_charger_power",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._ev_charger_pool.component_ids,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    async def stop(self) -> None:
        """Stop all tasks and channels owned by the EVChargerPool."""
        await self._ev_charger_pool.stop()

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
        """Return a receiver for the system power bounds."""
        return self._ev_charger_pool.bounds_channel
