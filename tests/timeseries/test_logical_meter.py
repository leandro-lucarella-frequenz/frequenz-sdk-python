# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from frequenz.channels import Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ChannelRegistry, ComponentMetricRequest
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.logical_meter import LogicalMeter
from frequenz.sdk.timeseries.logical_meter._resampled_formula_builder import (
    ResampledFormulaBuilder,
)

from .mock_microgrid import MockMicrogrid


class TestLogicalMeter:
    """Tests for the logical meter."""

    async def _get_resampled_stream(
        self,
        logical_meter: LogicalMeter,
        channel_registry: ChannelRegistry,
        request_sender: Sender[ComponentMetricRequest],
        comp_id: int,
    ) -> Receiver[Sample]:
        """Return the resampled data stream for the given component."""
        # Create a `FormulaBuilder` instance, just in order to reuse its
        # `_get_resampled_receiver` function implementation.

        # pylint: disable=protected-access
        builder = ResampledFormulaBuilder(
            logical_meter._namespace,
            channel_registry,
            request_sender,
            ComponentMetricId.ACTIVE_POWER,
        )
        return await builder._get_resampled_receiver(comp_id)
        # pylint: enable=protected-access

    async def test_grid_power_1(self, mocker: MockerFixture) -> None:
        """Test the grid power formula with a grid side meter."""
        mockgrid = await MockMicrogrid.new(mocker, grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        request_sender, channel_registry = await mockgrid.start()
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        main_meter_recv = await self._get_resampled_stream(
            logical_meter,
            channel_registry,
            request_sender,
            mockgrid.main_meter_id,
        )

        results = []
        main_meter_data = []
        for _ in range(10):
            val = await main_meter_recv.receive()
            assert val is not None and val.value is not None and val.value > 0.0
            main_meter_data.append(val.value)

            val = await grid_power_recv.receive()
            assert val is not None
            results.append(val.value)
        await mockgrid.cleanup()

        assert results == main_meter_data

    async def test_grid_power_2(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid power formula without a grid side meter."""
        mockgrid = await MockMicrogrid.new(mocker, grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        request_sender, channel_registry = await mockgrid.start()
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        meter_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
            )
            for meter_id in mockgrid.meter_ids
        ]

        results = []
        meter_sums = []
        for _ in range(10):
            meter_sum = 0.0
            for recv in meter_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                meter_sum += val.value

            val = await grid_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)
            meter_sums.append(meter_sum)

        await mockgrid.cleanup()

        assert len(results) == 10
        assert results == meter_sums

    async def test_battery_and_pv_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = await MockMicrogrid.new(mocker)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        request_sender, channel_registry = await mockgrid.start()
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()

        bat_inv_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
            )
            for meter_id in mockgrid.battery_inverter_ids
        ]

        pv_inv_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
            )
            for meter_id in mockgrid.pv_inverter_ids
        ]

        battery_results = []
        pv_results = []
        battery_inv_sums = []
        pv_inv_sums = []
        for _ in range(10):
            bat_inv_sum = 0.0
            pv_inv_sum = 0.0
            for recv in bat_inv_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                bat_inv_sum += val.value
            battery_inv_sums.append(bat_inv_sum)

            for recv in pv_inv_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                pv_inv_sum += val.value
            pv_inv_sums.append(pv_inv_sum)

            val = await battery_power_recv.receive()
            assert val is not None and val.value is not None
            battery_results.append(val.value)

            val = await pv_power_recv.receive()
            assert val is not None and val.value is not None
            pv_results.append(val.value)

        await mockgrid.cleanup()

        assert len(battery_results) == 10
        assert battery_results == battery_inv_sums
        assert len(pv_results) == 10
        assert pv_results == pv_inv_sums
