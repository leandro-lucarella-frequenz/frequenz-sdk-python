# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines a graph representation of how microgrid components are connected.

The component graph is an approximate representation of the microgrid circuit,
abstracted to a level appropriate for higher-level monitoring and control.
Examples of use-cases would be:

  * using the graph structure to infer which component measurements
    need to be combined to obtain grid power or onsite load

  * identifying which inverter(s) need to be engaged to (dis)charge
    a particular battery

  * understanding which power flows in the microgrid are derived from
    green and grey sources

It deliberately does not include all pieces of hardware placed in the microgrid,
instead limiting itself to just those that are needed to monitor and control the
flow of power.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable

import networkx as nx
from frequenz.client.microgrid import (
    ApiClient,
    Component,
    ComponentCategory,
    Connection,
    InverterType,
)

_logger = logging.getLogger(__name__)

# pylint: disable=too-many-lines


# Constant to store the actual obejcts as data attached to the graph nodes and edges
_DATA_KEY = "data"


class InvalidGraphError(Exception):
    """Exception type that will be thrown if graph data is not valid."""


class ComponentGraph(ABC):
    """Interface for component graph implementations."""

    @abstractmethod
    def components(
        self,
        component_ids: set[int] | None = None,
        component_categories: set[ComponentCategory] | None = None,
    ) -> set[Component]:
        """Fetch the components of the microgrid.

        Args:
            component_ids: filter out any components not matching one of the provided IDs
            component_categories: filter out any components not matching one of the
                provided types

        Returns:
            Set of the components currently connected to the microgrid, filtered by
                the provided `component_ids` and `component_categories` values.
        """

    @abstractmethod
    def connections(
        self,
        start: set[int] | None = None,
        end: set[int] | None = None,
    ) -> set[Connection]:
        """Fetch the connections between microgrid components.

        Args:
            start: filter out any connections whose `start` does not match one of these
                component IDs
            end: filter out any connections whose `end` does not match one of these
                component IDs

        Returns:
            Set of the connections between components in the microgrid, filtered by
                the provided `start`/`end` choices.
        """

    @abstractmethod
    def predecessors(self, component_id: int) -> set[Component]:
        """Fetch the graph predecessors of the specified component.

        Args:
            component_id: numerical ID of the component whose predecessors should be
                fetched

        Returns:
            Set of IDs of the components that are predecessors of `component_id`,
                i.e. for which there is a connection from each of these components to
                `component_id`.

        Raises:
            KeyError: if the specified `component_id` is not in the graph
        """

    @abstractmethod
    def successors(self, component_id: int) -> set[Component]:
        """Fetch the graph successors of the specified component.

        Args:
            component_id: numerical ID of the component whose successors should be
                fetched

        Returns:
            Set of IDs of the components that are successors of `component_id`,
                i.e. for which there is a connection from `component_id` to each of
                these components.

        Raises:
            KeyError: if the specified `component_id` is not in the graph
        """

    @abstractmethod
    def is_grid_meter(self, component: Component) -> bool:
        """Check if the specified component is a grid meter.

        This is done by checking if the component is the only successor to the `Grid`
        component.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a grid meter.
        """

    @abstractmethod
    def is_pv_inverter(self, component: Component) -> bool:
        """Check if the specified component is a PV inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a PV inverter.
        """

    @abstractmethod
    def is_pv_meter(self, component: Component) -> bool:
        """Check if the specified component is a PV meter.

        This is done by checking if the component has only PV inverters as its
        successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a PV meter.
        """

    @abstractmethod
    def is_pv_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a PV chain.

        A component is part of a PV chain if it is a PV meter or a PV inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a PV chain.
        """

    @abstractmethod
    def is_battery_inverter(self, component: Component) -> bool:
        """Check if the specified component is a battery inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a battery inverter.
        """

    @abstractmethod
    def is_battery_meter(self, component: Component) -> bool:
        """Check if the specified component is a battery meter.

        This is done by checking if the component has only battery inverters as its
        predecessors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a battery meter.
        """

    @abstractmethod
    def is_battery_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a battery chain.

        A component is part of a battery chain if it is a battery meter or a battery
        inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a battery chain.
        """

    @abstractmethod
    def is_ev_charger(self, component: Component) -> bool:
        """Check if the specified component is an EV charger.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is an EV charger.
        """

    @abstractmethod
    def is_ev_charger_meter(self, component: Component) -> bool:
        """Check if the specified component is an EV charger meter.

        This is done by checking if the component has only EV chargers as its
        successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is an EV charger meter.
        """

    @abstractmethod
    def is_ev_charger_chain(self, component: Component) -> bool:
        """Check if the specified component is part of an EV charger chain.

        A component is part of an EV charger chain if it is an EV charger meter or an
        EV charger.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of an EV charger chain.
        """

    @abstractmethod
    def is_chp(self, component: Component) -> bool:
        """Check if the specified component is a CHP.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a CHP.
        """

    @abstractmethod
    def is_chp_meter(self, component: Component) -> bool:
        """Check if the specified component is a CHP meter.

        This is done by checking if the component has only CHPs as its successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a CHP meter.
        """

    @abstractmethod
    def is_chp_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a CHP chain.

        A component is part of a CHP chain if it is a CHP meter or a CHP.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a CHP chain.
        """

    @abstractmethod
    def dfs(
        self,
        current_node: Component,
        visited: set[Component],
        condition: Callable[[Component], bool],
    ) -> set[Component]:
        """
        Search for components that fulfill the condition in the Graph.

        DFS is used for searching the graph. The graph traversal is stopped
        once a component fulfills the condition.

        Args:
            current_node: The current node to search from.
            visited: The set of visited nodes.
            condition: The condition function to check for.

        Returns:
            A set of component ids where the corresponding components fulfill
            the condition function.
        """

    @abstractmethod
    def find_first_descendant_component(
        self,
        *,
        root_category: ComponentCategory,
        descendant_categories: Iterable[ComponentCategory],
    ) -> Component:
        """Find the first descendant component given root and descendant categories.

        This method searches for the root component within the provided root
        category. If multiple components share the same root category, the
        first found one is considered as the root component.

        Subsequently, it looks for the first descendant component from the root
        component, considering only the immediate descendants.

        The priority of the component to search for is determined by the order
        of the descendant categories, with the first category having the
        highest priority.

        Args:
            root_category: The category of the root component to search for.
            descendant_categories: The descendant categories to search for the
                first descendant component in.

        Returns:
            The first descendant component found in the component graph,
            considering the specified root and descendant categories.
        """


class _MicrogridComponentGraph(
    ComponentGraph
):  # pylint: disable=too-many-public-methods
    """ComponentGraph implementation designed to work with the microgrid API.

    For internal-only use of the `microgrid` package.
    """

    def __init__(
        self,
        components: set[Component] | None = None,
        connections: set[Connection] | None = None,
    ) -> None:
        """Initialize the component graph.

        Args:
            components: components with which to first initialize the graph,
                provided as pairs of the form `(component_id,
                component_category)`; if set, must provide `connections` as well
            connections: connections with which to initialize the graph,
                provided as pairs of component IDs describing the start and end
                of the connection; if set, must provide `components` as well

        Raises:
            InvalidGraphError: if `components` and `connections` are not both `None`
                and either of them is either `None` or empty
        """
        self._graph: nx.DiGraph = nx.DiGraph()

        if components is None and connections is None:
            return

        if components is None or len(components) == 0:
            raise InvalidGraphError("Must provide components as well as connections")

        if connections is None or len(connections) == 0:
            raise InvalidGraphError("Must provide connections as well as components")

        self.refresh_from(components, connections)
        self.validate()

    def components(
        self,
        component_ids: set[int] | None = None,
        component_categories: set[ComponentCategory] | None = None,
    ) -> set[Component]:
        """Fetch the components of the microgrid.

        Args:
            component_ids: filter out any components not matching one of the provided IDs
            component_categories: filter out any components not matching one of the
                provided types

        Returns:
            Set of the components currently connected to the microgrid, filtered by
                the provided `component_ids` and `component_categories` values.
        """
        selection_ids = (
            self._graph.nodes
            if component_ids is None
            else component_ids & self._graph.nodes
        )
        selection: Iterable[Component] = (
            self._graph.nodes[i][_DATA_KEY] for i in selection_ids
        )

        if component_categories is not None:
            selection = filter(lambda c: c.category in component_categories, selection)

        return set(selection)

    def connections(
        self,
        start: set[int] | None = None,
        end: set[int] | None = None,
    ) -> set[Connection]:
        """Fetch the connections between microgrid components.

        Args:
            start: filter out any connections whose `start` does not match one of these
                component IDs
            end: filter out any connections whose `end` does not match one of these
                component IDs

        Returns:
            Set of the connections between components in the microgrid, filtered by
                the provided `start`/`end` choices.
        """
        match (start, end):
            case (None, None):
                selection_ids = self._graph.edges
            case (None, _):
                selection_ids = self._graph.in_edges(end)
            case (_, None):
                selection_ids = self._graph.out_edges(start)
            case (_, _):
                start_edges = self._graph.out_edges(start)
                end_edges = self._graph.in_edges(end)
                selection_ids = set(start_edges).intersection(end_edges)

        return set(self._graph.edges[i][_DATA_KEY] for i in selection_ids)

    def predecessors(self, component_id: int) -> set[Component]:
        """Fetch the graph predecessors of the specified component.

        Args:
            component_id: numerical ID of the component whose predecessors should be
                fetched

        Returns:
            Set of IDs of the components that are predecessors of `component_id`,
                i.e. for which there is a connection from each of these components to
                `component_id`.

        Raises:
            KeyError: if the specified `component_id` is not in the graph
        """
        if component_id not in self._graph:
            raise KeyError(
                f"Component {component_id} not in graph, cannot get predecessors!"
            )

        predecessors_ids = self._graph.predecessors(component_id)

        return set(map(lambda idx: self._graph.nodes[idx][_DATA_KEY], predecessors_ids))

    def successors(self, component_id: int) -> set[Component]:
        """Fetch the graph successors of the specified component.

        Args:
            component_id: numerical ID of the component whose successors should be
                fetched

        Returns:
            Set of IDs of the components that are successors of `component_id`,
                i.e. for which there is a connection from `component_id` to each of
                these components.

        Raises:
            KeyError: if the specified `component_id` is not in the graph
        """
        if component_id not in self._graph:
            raise KeyError(
                f"Component {component_id} not in graph, cannot get successors!"
            )

        successors_ids = self._graph.successors(component_id)

        return set(map(lambda idx: self._graph.nodes[idx][_DATA_KEY], successors_ids))

    def refresh_from(
        self,
        components: set[Component],
        connections: set[Connection],
        correct_errors: Callable[["_MicrogridComponentGraph"], None] | None = None,
    ) -> None:
        """Refresh the graph from the provided list of components and connections.

        This will completely overwrite the current graph data with the provided
        components and connections.

        Args:
            components: components to include in the graph, provided as pairs of
                the form `(component_id, component_category)`
            connections: connections to include in the graph, provided as pairs
                of component IDs describing the start and end of the connection
            correct_errors: callback that, if set, will be invoked if the
                provided graph data is in any way invalid (it will attempt to
                correct the errors by inferring what the correct data should be)

        Raises:
            InvalidGraphError: if the provided `components` and `connections`
                do not form a valid component graph and `correct_errors` does
                not fix it.
        """
        if not all(component.is_valid() for component in components):
            raise InvalidGraphError(f"Invalid components in input: {components}")
        if not all(connection.is_valid() for connection in connections):
            raise InvalidGraphError(f"Invalid connections in input: {connections}")

        new_graph = nx.DiGraph()
        for component in components:
            new_graph.add_node(component.component_id, **{_DATA_KEY: component})

        # Store the original connection object in the edge data (third item in the
        # tuple) so that we can retrieve it later.
        for connection in connections:
            new_graph.add_edge(
                connection.start, connection.end, **{_DATA_KEY: connection}
            )

        # check if we can construct a valid ComponentGraph
        # from the new NetworkX graph data
        _provisional = _MicrogridComponentGraph()
        _provisional._graph = new_graph  # pylint: disable=protected-access
        if correct_errors is not None:
            try:
                _provisional.validate()
            except InvalidGraphError as err:
                _logger.warning("Attempting to fix invalid component data: %s", err)
                correct_errors(_provisional)

        try:
            _provisional.validate()
        except Exception as err:
            _logger.error("Failed to parse component graph: %s", err)
            raise InvalidGraphError(
                "Cannot populate component graph from provided input!"
            ) from err

        old_graph = self._graph
        self._graph = new_graph
        old_graph.clear()  # just in case any references remain, but should not

    async def refresh_from_api(
        self,
        api: ApiClient,
        correct_errors: Callable[["_MicrogridComponentGraph"], None] | None = None,
    ) -> None:
        """Refresh the contents of a component graph from the remote API.

        Args:
            api: API client from which to fetch graph data
            correct_errors: callback that, if set, will be invoked if the
                provided graph data is in any way invalid (it will attempt to
                correct the errors by inferring what the correct data should be)
        """
        components, connections = await asyncio.gather(
            api.components(),
            api.connections(),
        )

        self.refresh_from(set(components), set(connections), correct_errors)

    def validate(self) -> None:
        """Check that the component graph contains valid microgrid data."""
        self._validate_graph()
        self._validate_graph_root()
        self._validate_grid_endpoint()
        self._validate_intermediary_components()
        self._validate_leaf_components()

    def is_grid_meter(self, component: Component) -> bool:
        """Check if the specified component is a grid meter.

        This is done by checking if the component is the only successor to the `Grid`
        component.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a grid meter.
        """
        if component.category != ComponentCategory.METER:
            return False

        predecessors = self.predecessors(component.component_id)
        if len(predecessors) != 1:
            return False

        predecessor = next(iter(predecessors))
        if predecessor.category != ComponentCategory.GRID:
            return False

        grid_successors = self.successors(predecessor.component_id)
        return len(grid_successors) == 1

    def is_pv_inverter(self, component: Component) -> bool:
        """Check if the specified component is a PV inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a PV inverter.
        """
        return (
            component.category == ComponentCategory.INVERTER
            and component.type == InverterType.SOLAR
        )

    def is_pv_meter(self, component: Component) -> bool:
        """Check if the specified component is a PV meter.

        This is done by checking if the component has only PV inverters as its
        successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a PV meter.
        """
        successors = self.successors(component.component_id)
        return (
            component.category == ComponentCategory.METER
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(
                self.is_pv_inverter(successor)
                for successor in self.successors(component.component_id)
            )
        )

    def is_pv_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a PV chain.

        A component is part of a PV chain if it is either a PV inverter or a PV
        meter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a PV chain.
        """
        return self.is_pv_inverter(component) or self.is_pv_meter(component)

    def is_ev_charger(self, component: Component) -> bool:
        """Check if the specified component is an EV charger.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is an EV charger.
        """
        return component.category == ComponentCategory.EV_CHARGER

    def is_ev_charger_meter(self, component: Component) -> bool:
        """Check if the specified component is an EV charger meter.

        This is done by checking if the component has only EV chargers as its
        successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is an EV charger meter.
        """
        successors = self.successors(component.component_id)
        return (
            component.category == ComponentCategory.METER
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_ev_charger(successor) for successor in successors)
        )

    def is_ev_charger_chain(self, component: Component) -> bool:
        """Check if the specified component is part of an EV charger chain.

        A component is part of an EV charger chain if it is either an EV charger or an
        EV charger meter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of an EV charger chain.
        """
        return self.is_ev_charger(component) or self.is_ev_charger_meter(component)

    def is_battery_inverter(self, component: Component) -> bool:
        """Check if the specified component is a battery inverter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a battery inverter.
        """
        return (
            component.category == ComponentCategory.INVERTER
            and component.type == InverterType.BATTERY
        )

    def is_battery_meter(self, component: Component) -> bool:
        """Check if the specified component is a battery meter.

        This is done by checking if the component has only battery inverters as
        its successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a battery meter.
        """
        successors = self.successors(component.component_id)
        return (
            component.category == ComponentCategory.METER
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_battery_inverter(successor) for successor in successors)
        )

    def is_battery_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a battery chain.

        A component is part of a battery chain if it is either a battery inverter or a
        battery meter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a battery chain.
        """
        return self.is_battery_inverter(component) or self.is_battery_meter(component)

    def is_chp(self, component: Component) -> bool:
        """Check if the specified component is a CHP.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a CHP.
        """
        return component.category == ComponentCategory.CHP

    def is_chp_meter(self, component: Component) -> bool:
        """Check if the specified component is a CHP meter.

        This is done by checking if the component has only CHPs as its
        successors.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is a CHP meter.
        """
        successors = self.successors(component.component_id)
        return (
            component.category == ComponentCategory.METER
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_chp(successor) for successor in successors)
        )

    def is_chp_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a CHP chain.

        A component is part of a CHP chain if it is either a CHP or a CHP meter.

        Args:
            component: component to check.

        Returns:
            Whether the specified component is part of a CHP chain.
        """
        return self.is_chp(component) or self.is_chp_meter(component)

    def dfs(
        self,
        current_node: Component,
        visited: set[Component],
        condition: Callable[[Component], bool],
    ) -> set[Component]:
        """
        Search for components that fulfill the condition in the Graph.

        DFS is used for searching the graph. The graph traversal is stopped
        once a component fulfills the condition.

        Args:
            current_node: The current node to search from.
            visited: The set of visited nodes.
            condition: The condition function to check for.

        Returns:
            A set of component ids where the corresponding components fulfill
            the condition function.
        """
        if current_node in visited:
            return set()

        visited.add(current_node)

        if condition(current_node):
            return {current_node}

        component: set[Component] = set()

        for successor in self.successors(current_node.component_id):
            component.update(self.dfs(successor, visited, condition))

        return component

    def find_first_descendant_component(
        self,
        *,
        root_category: ComponentCategory,
        descendant_categories: Iterable[ComponentCategory],
    ) -> Component:
        """Find the first descendant component given root and descendant categories.

        This method searches for the root component within the provided root
        category. If multiple components share the same root category, the
        first found one is considered as the root component.

        Subsequently, it looks for the first descendant component from the root
        component, considering only the immediate descendants.

        The priority of the component to search for is determined by the order
        of the descendant categories, with the first category having the
        highest priority.

        Args:
            root_category: The category of the root component to search for.
            descendant_categories: The descendant categories to search for the
                first descendant component in.

        Raises:
            ValueError: when the root component is not found in the component
                graph or when no component is found in the given categories.

        Returns:
            The first descendant component found in the component graph,
            considering the specified root and descendant categories.
        """
        root_component = next(
            (comp for comp in self.components(component_categories={root_category})),
            None,
        )

        if root_component is None:
            raise ValueError(f"Root component not found for {root_category.name}")

        # Sort by component ID to ensure consistent results.
        successors = sorted(
            self.successors(root_component.component_id),
            key=lambda comp: comp.component_id,
        )

        def find_component(component_category: ComponentCategory) -> Component | None:
            return next(
                (comp for comp in successors if comp.category == component_category),
                None,
            )

        # Find the first component that matches the given descendant categories
        # in the order of the categories list.
        component = next(filter(None, map(find_component, descendant_categories)), None)

        if component is None:
            raise ValueError("Component not found in any of the descendant categories.")

        return component

    def _validate_graph(self) -> None:
        """Check that the underlying graph data is valid.

        Raises:
            InvalidGraphError: if there are no components, or no connections, or
                the graph is not a tree, or if any component lacks type data
        """
        if self._graph.number_of_nodes() == 0:
            raise InvalidGraphError("No components in graph!")

        if self._graph.number_of_edges() == 0:
            raise InvalidGraphError("No connections in component graph!")

        if not nx.is_directed_acyclic_graph(self._graph):
            raise InvalidGraphError("Component graph is not a tree!")

        # This check doesn't seem to have much sense, it only search for nodes without
        # data associated with them. We leave it here for now, but we should consider
        # removing it in the future.
        undefined: list[int] = [
            node[0] for node in self._graph.nodes(data=True) if len(node[1]) == 0
        ]

        if len(undefined) > 0:
            raise InvalidGraphError(
                f"Missing definition for graph components: {undefined}"
            )
        # should be true as a consequence of checks above
        if sum(1 for _ in self.components()) <= 0:
            raise InvalidGraphError("Graph must have a least one component!")
        if sum(1 for _ in self.connections()) <= 0:
            raise InvalidGraphError("Graph must have a least one connection!")

        # should be true as a consequence of the tree property:
        # there should be no unconnected components
        unconnected = filter(
            lambda c: self._graph.degree(c.component_id) == 0, self.components()
        )
        if sum(1 for _ in unconnected) != 0:
            raise InvalidGraphError(
                "Every component must have at least one connection!"
            )

    def _validate_graph_root(self) -> None:
        """Check that there is exactly one node without predecessors, of valid type.

        Raises:
            InvalidGraphError: if there is more than one node without predecessors,
                or if there is a single such node that is not one of NONE, GRID, or
                JUNCTION
        """
        no_predecessors = filter(
            lambda c: self._graph.in_degree(c.component_id) == 0,
            self.components(),
        )

        valid_root_types = {
            ComponentCategory.NONE,
            ComponentCategory.GRID,
        }

        valid_roots = list(
            filter(lambda c: c.category in valid_root_types, no_predecessors)
        )

        if len(valid_roots) == 0:
            raise InvalidGraphError("No valid root nodes of component graph!")

        if len(valid_roots) > 1:
            raise InvalidGraphError(f"Multiple potential root nodes: {valid_roots}")

        root = valid_roots[0]
        if self._graph.out_degree(root.component_id) == 0:
            raise InvalidGraphError(f"Graph root {root} has no successors!")

    def _validate_grid_endpoint(self) -> None:
        """Check that the grid endpoint is configured correctly in the graph.

        Raises:
            InvalidGraphError: if there is more than one grid endpoint in the
                graph, or if the grid endpoint has predecessors (if it exists,
                then it should be the root of the component-graph tree), or if
                it has no successors in the graph (i.e. it is not connected to
                anything)
        """
        grid = list(self.components(component_categories={ComponentCategory.GRID}))

        if len(grid) == 0:
            # it's OK to not have a grid endpoint as long as other properties
            # (checked by other `_validate...` methods) hold
            return

        if len(grid) > 1:
            raise InvalidGraphError(
                f"Multiple grid endpoints in component graph: {grid}"
            )

        grid_id = grid[0].component_id
        if self._graph.in_degree(grid_id) > 0:
            grid_predecessors = list(self.predecessors(grid_id))
            raise InvalidGraphError(
                f"Grid endpoint {grid_id} has graph predecessors: {grid_predecessors}"
            )

        if self._graph.out_degree(grid_id) == 0:
            raise InvalidGraphError(f"Grid endpoint {grid_id} has no graph successors!")

    def _validate_intermediary_components(self) -> None:
        """Check that intermediary components (e.g. meters) are configured correctly.

        Intermediary components are components that should have both predecessors and
        successors in the component graph, such as METER, or INVERTER.

        Raises:
            InvalidGraphError: if any intermediary component has zero predecessors
                or zero successors
        """
        intermediary_components = list(
            self.components(component_categories={ComponentCategory.INVERTER})
        )

        missing_predecessors = list(
            filter(
                lambda c: sum(1 for _ in self.predecessors(c.component_id)) == 0,
                intermediary_components,
            )
        )
        if len(missing_predecessors) > 0:
            raise InvalidGraphError(
                "Intermediary components without graph predecessors: "
                f"{missing_predecessors}"
            )

    def _validate_leaf_components(self) -> None:
        """Check that leaf components (e.g. batteries) are configured correctly.

        Leaf components are components that should be leaves of the component-graph
        tree, such as LOAD, BATTERY or EV_CHARGER.  These should have only incoming
        connections and no outgoing connections.

        Raises:
            InvalidGraphError: if any leaf component in the graph has 0 predecessors,
                or has > 0 successors
        """
        leaf_components = list(
            self.components(
                component_categories={
                    ComponentCategory.BATTERY,
                    ComponentCategory.EV_CHARGER,
                }
            )
        )

        missing_predecessors = list(
            filter(
                lambda c: sum(1 for _ in self.predecessors(c.component_id)) == 0,
                leaf_components,
            )
        )
        if len(missing_predecessors) > 0:
            raise InvalidGraphError(
                f"Leaf components without graph predecessors: {missing_predecessors}"
            )

        with_successors = list(
            filter(
                lambda c: sum(1 for _ in self.successors(c.component_id)) > 0,
                leaf_components,
            )
        )
        if len(with_successors) > 0:
            raise InvalidGraphError(
                f"Leaf components with graph successors: {with_successors}"
            )
