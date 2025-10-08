from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

import networkx as nx

from app.db.sql_alchemy import connection_scope, get_inspector


@dataclass(frozen=True)
class ForeignKeyConstraint:
    """Represents a single foreign key relationship between two tables."""

    name: str | None
    from_table: str
    to_table: str
    column_pairs: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class JoinStep:
    """A single hop in a join path between two tables."""

    left_table: str
    right_table: str
    column_pairs: tuple[tuple[str, str], ...]
    constraint_name: str | None


class FKAnalyzer:
    """Builds and queries a graph of foreign key relationships for a database."""

    @classmethod
    def shortest_join_path(cls, database: str, tables: Iterable[str]) -> list[JoinStep]:
        """Return the shortest join path that connects all requested tables.

        For two tables this is the standard shortest path. For more than two tables
        the method computes an approximate Steiner tree that connects the tables with
        the minimal number of joins. A ``ValueError`` is raised if any table is
        unknown or if no join path exists between the requested tables.
        """

        requested = tuple(dict.fromkeys(tables))
        if len(requested) < 2:
            msg = "Provide at least two table names to compute a join path."
            raise ValueError(msg)

        graph = cls._get_graph(database)
        cls._ensure_tables_exist(graph, requested)

        if len(requested) == 2:
            return cls._pairwise_path(graph, requested[0], requested[1])

        steiner = nx.algorithms.approximation.steiner_tree(graph, set(requested))
        if steiner.number_of_edges() == 0:
            msg = f"No join path connects the tables: {', '.join(requested)}."
            raise ValueError(msg)

        steps: list[JoinStep] = []
        for left, right in nx.bfs_edges(steiner, source=requested[0]):
            steps.append(cls._edge_to_step(graph, left, right))
        return steps

    @classmethod
    def clear_cache(cls) -> None:
        """Reset the cached graph builder."""

        cls._get_graph.cache_clear()  # type: ignore[attr-defined]

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_graph(database: str) -> nx.Graph:
        with connection_scope(database) as conn:
            inspector = get_inspector(conn)
            graph = nx.Graph()
            tables = inspector.get_table_names()
            for table in tables:
                graph.add_node(table)
                for fk in inspector.get_foreign_keys(table):
                    referred_table = fk.get("referred_table")
                    constrained_columns = fk.get("constrained_columns") or []
                    referred_columns = fk.get("referred_columns") or []
                    if referred_table is None or not constrained_columns:
                        continue

                    column_pairs = tuple(zip(constrained_columns, referred_columns))
                    constraint = ForeignKeyConstraint(
                        name=fk.get("name"),
                        from_table=table,
                        to_table=referred_table,
                        column_pairs=column_pairs,
                    )

                    if graph.has_edge(table, referred_table):
                        graph[table][referred_table]["constraints"].append(constraint)
                    else:
                        graph.add_edge(
                            table,
                            referred_table,
                            weight=1,
                            constraints=[constraint],
                        )

            return graph

    @staticmethod
    def _ensure_tables_exist(graph: nx.Graph, tables: Iterable[str]) -> None:
        missing = [table for table in tables if table not in graph]
        if missing:
            available = ", ".join(sorted(graph.nodes))
            joined = ", ".join(missing)
            msg = f"Unknown tables: {joined}. Available tables: {available}."
            raise ValueError(msg)

    @classmethod
    def _pairwise_path(cls, graph: nx.Graph, left: str, right: str) -> list[JoinStep]:
        try:
            node_path = nx.shortest_path(graph, left, right, weight="weight")
        except nx.NetworkXNoPath as exc:
            msg = f"No join path between '{left}' and '{right}'."
            raise ValueError(msg) from exc

        steps: list[JoinStep] = []
        for idx in range(len(node_path) - 1):
            origin = node_path[idx]
            destination = node_path[idx + 1]
            steps.append(cls._edge_to_step(graph, origin, destination))
        return steps

    @staticmethod
    def _edge_to_step(graph: nx.Graph, left: str, right: str) -> JoinStep:
        edge_attrs = graph.get_edge_data(left, right)
        if edge_attrs is None:
            msg = f"No foreign key relationship between '{left}' and '{right}'."
            raise ValueError(msg)

        constraints: list[ForeignKeyConstraint] = edge_attrs["constraints"]
        for constraint in constraints:
            if constraint.from_table == left and constraint.to_table == right:
                return JoinStep(
                    left_table=left,
                    right_table=right,
                    column_pairs=constraint.column_pairs,
                    constraint_name=constraint.name,
                )
            if constraint.from_table == right and constraint.to_table == left:
                reversed_pairs = tuple(
                    (to_col, from_col) for from_col, to_col in constraint.column_pairs
                )
                return JoinStep(
                    left_table=left,
                    right_table=right,
                    column_pairs=reversed_pairs,
                    constraint_name=constraint.name,
                )

        constraint_names = ", ".join(
            constraint.name or "<unnamed>" for constraint in constraints
        )
        msg = (
            "No constraint orientation matches the requested edge between "
            f"'{left}' and '{right}'. Known constraints: {constraint_names}."
        )
        raise ValueError(msg)
