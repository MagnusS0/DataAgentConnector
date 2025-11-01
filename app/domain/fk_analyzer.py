from dataclasses import dataclass
from typing import Iterable, Protocol
from functools import lru_cache

import numpy as np
from scipy.sparse import coo_matrix, csr_matrix, csr_array
from scipy.sparse.csgraph import (
    shortest_path,
    connected_components,
    minimum_spanning_tree,
)

from app.core.logging import get_logger

logger = get_logger("domain.fk_analyzer")


class ForeignKeyInfo(Protocol):
    """Protocol for foreign key metadata."""

    def get_table_names(self) -> list[str]:
        """Return all table names in the database."""
        ...

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        """Return foreign key constraints for a table."""
        ...

    def __hash__(self) -> int: ...


@dataclass(frozen=True)
class ForeignKeyConstraint:
    name: str | None
    from_table: str
    to_table: str
    column_pairs: tuple[tuple[str, str], ...]  # (from_col, to_col)


@dataclass(frozen=True)
class JoinStep:
    left_table: str
    right_table: str
    column_pairs: tuple[tuple[str, str], ...]
    constraint_name: str | None


@dataclass(frozen=True)
class FKSnapshot:
    csr: csr_array
    name_to_idx: dict[str, int]
    idx_to_name: list[str]
    components: np.ndarray
    edge_constraints: dict[frozenset[str], list[ForeignKeyConstraint]]


@lru_cache(maxsize=16)
def _build_snapshot(fk_info: ForeignKeyInfo) -> FKSnapshot:
    """
    Build a snapshot of the foreign key graph from FK metadata.

    Constructs:
        - CSR adjacency matrix A ∈ {0,1}^{n*n}
          where A[i,j] = 1 if a join (FK) exists between tables i and j.
        - Connected components using `connected_components(A)`.
        - Edge constraints registry: for each {a,b}, all FK constraints.
    """
    tables = list(fk_info.get_table_names())

    name_to_idx = {t: i for i, t in enumerate(tables)}
    idx_to_name = tables[:]

    rows: list[int] = []
    cols: list[int] = []
    data: list[int] = []
    edge_constraints: dict[frozenset[str], list[ForeignKeyConstraint]] = {}

    missing_refs: set[tuple[str, str]] = set()

    for t in tables:
        for fk in fk_info.get_foreign_keys(t) or ():
            ref = fk.get("referred_table")
            cons = tuple(fk.get("constrained_columns") or ())
            refs = tuple(fk.get("referred_columns") or ())
            if not ref or not cons:
                continue

            if ref not in name_to_idx:
                # skip dangling FK targets
                missing_refs.add((t, ref))
                continue

            c = ForeignKeyConstraint(
                name=fk.get("name"),
                from_table=t,
                to_table=ref,
                column_pairs=tuple(zip(cons, refs)),
            )
            edge_constraints.setdefault(frozenset({t, ref}), []).append(c)

            # undirected unit edge for BFS/Dijkstra
            ui, vi = name_to_idx[t], name_to_idx[ref]
            rows.extend([ui, vi])
            cols.extend([vi, ui])
            data.extend([1, 1])

    n = len(tables)
    csr = coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()
    _, comps = connected_components(csr, directed=False)

    if missing_refs:
        missing_pairs = ", ".join(f"{src}->{dst}" for src, dst in sorted(missing_refs))
        logger.warning(
            "Foreign key references unknown table(s) skipped: %s", missing_pairs
        )

    return FKSnapshot(csr, name_to_idx, idx_to_name, comps, edge_constraints)


def clear_cache() -> None:
    """Clear cached FK snapshots."""
    _build_snapshot.cache_clear()


def shortest_join_path(
    fk_info: ForeignKeyInfo, left: str, right: str
) -> list[JoinStep]:
    """Return the shortest join path between two tables.

    Args:
        fk_info: Provider of table names and foreign key metadata.
        left: The starting table name.
        right: The ending table name.

    Returns:
        A list of JoinStep objects representing the path.

    Raises:
        ValueError: If no path exists or if tables are unknown.
    """
    fk_graph = _build_snapshot(fk_info)
    _ensure_known(fk_graph, (left, right))
    source_index, target_index = fk_graph.name_to_idx[left], fk_graph.name_to_idx[right]
    if fk_graph.components[source_index] != fk_graph.components[target_index]:
        raise ValueError(f"No join path between '{left}' and '{right}'.")

    dist, pred = shortest_path(
        fk_graph.csr,
        directed=False,
        unweighted=True,
        indices=source_index,
        return_predecessors=True,
    )
    if not np.isfinite(dist[target_index]):
        raise ValueError(f"No join path between '{left}' and '{right}'.")

    path = _reconstruct_path(pred, source_index, target_index)
    return _steps_from_path(fk_graph, path)


def connect_tables(fk_info: ForeignKeyInfo, tables: Iterable[str]) -> list[JoinStep]:
    """Return a minimal join network connecting all given tables.

    This method computes an approximate Steiner tree to find the minimal
    set of joins needed to connect all tables in the `tables` argument.

    Args:
        fk_info: Provider of table names and foreign key metadata.
        tables: An iterable of table names to connect.

    Returns:
        A list of JoinStep objects representing the connection path.

    Raises:
        ValueError: If no path exists, tables are unknown, or < 2 tables are given.
    """
    requested = list(dict.fromkeys(tables))
    if len(requested) < 2:
        raise ValueError("Provide at least two tables.")
    fk_graph = _build_snapshot(fk_info)
    _ensure_known(fk_graph, requested)

    term_idx = [fk_graph.name_to_idx[t] for t in requested]
    if len(set(map(int, fk_graph.components[term_idx]))) > 1:
        raise ValueError(f"No join path connects: {', '.join(requested)}.")

    # Distances from terminals
    dists, preds = shortest_path(
        fk_graph.csr,
        directed=False,
        unweighted=True,
        indices=term_idx,
        return_predecessors=True,
    )
    k = len(term_idx)
    W = np.full((k, k), np.inf)
    for i in range(k):
        for j in range(i + 1, k):
            w = dists[i, term_idx[j]]
            if not np.isfinite(w):
                raise ValueError(f"No join path connects: {', '.join(requested)}.")
            W[i, j] = W[j, i] = w

    # MST on the complete terminal graph
    mst = minimum_spanning_tree(csr_matrix(W)).tocoo()

    # Expand MST edges back to original graph
    edges: set[tuple[int, int]] = set()
    for i, j in zip(mst.row, mst.col):
        si, tj = term_idx[i], term_idx[j]
        path = _reconstruct_path(preds[i], si, tj)
        edges.update({(u, v) for u, v in zip(path, path[1:])})
        edges.update({(v, u) for u, v in zip(path, path[1:])})  # undirected

    # BFS order over steiner edges starting at first terminal
    ordered = _bfs_edges_from(term_idx[0], edges)
    return [_edge_to_step(fk_graph, u, v) for (u, v) in ordered]


def _ensure_known(g: FKSnapshot, names: Iterable[str]) -> None:
    """Raise ValueError if any table names are not in the graph."""
    missing = [t for t in names if t not in g.name_to_idx]
    if missing:
        avail = ", ".join(sorted(g.name_to_idx))
        raise ValueError(f"Unknown tables: {', '.join(missing)}. Available: {avail}.")


def _reconstruct_path(pred_row: np.ndarray, s: int, t: int) -> list[int]:
    """Reconstruct a path from a scipy predecessor matrix."""
    path: list[int] = []
    cur = int(t)
    while cur != -9999 and cur != s:
        path.append(cur)
        cur = int(pred_row[cur])
    path.append(s)
    path.reverse()
    return path


def _edge_to_step(g: FKSnapshot, u: int, v: int) -> JoinStep:
    """Convert a graph edge (u, v) to a JoinStep."""
    a, b = g.idx_to_name[u], g.idx_to_name[v]
    cs = g.edge_constraints.get(frozenset({a, b})) or []
    for c in cs:
        if c.from_table == a and c.to_table == b:
            return JoinStep(a, b, c.column_pairs, c.name)
        if c.from_table == b and c.to_table == a:
            pairs = tuple((to_col, from_col) for (from_col, to_col) in c.column_pairs)
            return JoinStep(a, b, pairs, c.name)
    names = ", ".join(c.name or "<unnamed>" for c in cs) or "<none>"
    raise ValueError(
        f"No oriented FK fits edge '{a}' → '{b}'. Known constraints: {names}."
    )


def _steps_from_path(g: FKSnapshot, path: list[int]) -> list[JoinStep]:
    """Convert a list of node indices into a list of JoinSteps."""
    return [_edge_to_step(g, u, v) for u, v in zip(path, path[1:])]


def _bfs_edges_from(
    start: int, undirected_edges: set[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Return a BFS-ordered list of directed edges from a set of undirected edges."""
    adj: dict[int, list[int]] = {}
    for u, v in undirected_edges:
        adj.setdefault(u, []).append(v)

    seen: set[int] = {start}
    q: list[int] = [start]
    out: list[tuple[int, int]] = []

    while q:
        u = q.pop(0)
        for v in adj.get(u, []):
            if v not in seen:
                seen.add(v)
                q.append(v)
                out.append((u, v))
    return out
