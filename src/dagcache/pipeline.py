"""Pipeline: a set of stages and the DAG their declared paths imply.

Edges are inferred, never declared: stage B depends on stage A when one of B's
``deps`` equals, contains, or lies inside one of A's ``outs``.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass

from .errors import PipelineError
from .params import DEFAULT_PARAMS_FILE
from .paths import overlaps
from .stage import Stage, StageFunc


@dataclass(frozen=True)
class Graph:
    """Validated adjacency of a pipeline."""

    order: tuple[str, ...]
    upstream: dict[str, frozenset[str]]
    downstream: dict[str, frozenset[str]]
    #: (stage, dep path) -> list of (producer stage, produced out path)
    producers: dict[tuple[str, str], tuple[tuple[str, str], ...]]


class Pipeline:
    """An ordered collection of :class:`Stage` objects.

    Stages can be added directly with :meth:`add` or with the :meth:`stage`
    decorator::

        pipe = Pipeline("demo")

        @pipe.stage(deps=["data/raw.csv"], outs=["data/clean.csv"], params=["clean.drop_na"])
        def clean(ctx):
            ...
    """

    def __init__(self, name: str = "pipeline", *, params_file: str = DEFAULT_PARAMS_FILE) -> None:
        self.name = name
        self.params_file = params_file
        self._stages: dict[str, Stage] = {}
        self._graph: Graph | None = None

    # -- construction ----------------------------------------------------
    def add(self, stage: Stage) -> Stage:
        if stage.name in self._stages:
            raise PipelineError(f"duplicate stage name '{stage.name}'")
        self._stages[stage.name] = stage
        self._graph = None
        return stage

    def stage(
        self,
        *,
        name: str | None = None,
        deps: Sequence[str] = (),
        outs: Sequence[str] = (),
        params: Sequence[str] = (),
        code: Sequence[str] = (),
        env: Sequence[str] = (),
        always_run: bool = False,
        salt: str = "",
    ) -> Callable[[StageFunc], StageFunc]:
        """Decorator registering a Python function as a stage."""

        def register(fn: StageFunc) -> StageFunc:
            self.add(
                Stage(
                    name=name or fn.__name__,
                    deps=tuple(deps),
                    outs=tuple(outs),
                    params=tuple(params),
                    code=tuple(code),
                    env=tuple(env),
                    func=fn,
                    always_run=always_run,
                    salt=salt,
                    description=(fn.__doc__ or "").strip().splitlines()[0] if fn.__doc__ else "",
                )
            )
            return fn

        return register

    # -- access ----------------------------------------------------------
    def __len__(self) -> int:
        return len(self._stages)

    def __iter__(self) -> Iterator[Stage]:
        return iter(self._stages.values())

    def __contains__(self, name: object) -> bool:
        return name in self._stages

    def get(self, name: str) -> Stage:
        try:
            return self._stages[name]
        except KeyError:
            known = ", ".join(self._stages) or "<none>"
            raise PipelineError(f"unknown stage '{name}' (known: {known})") from None

    @property
    def names(self) -> list[str]:
        return list(self._stages)

    # -- graph -------------------------------------------------------------
    @property
    def graph(self) -> Graph:
        if self._graph is None:
            self._graph = self._build_graph()
        return self._graph

    def validate(self) -> Graph:
        """Build and return the graph, raising :class:`PipelineError` if invalid."""
        return self.graph

    def _build_graph(self) -> Graph:
        stages = list(self._stages.values())
        # Outputs must be disjoint across and within stages.
        owners: list[tuple[str, str]] = []
        for st in stages:
            for out in st.outs:
                for other_stage, other_out in owners:
                    if overlaps(out, other_out):
                        raise PipelineError(
                            f"output '{out}' of stage '{st.name}' overlaps output "
                            f"'{other_out}' of stage '{other_stage}'"
                        )
                owners.append((st.name, out))
            for dep in st.deps:
                for out in st.outs:
                    if overlaps(dep, out):
                        raise PipelineError(
                            f"stage '{st.name}' depends on its own output '{out}' (via '{dep}')"
                        )
        producers: dict[tuple[str, str], tuple[tuple[str, str], ...]] = {}
        upstream: dict[str, set[str]] = {s.name: set() for s in stages}
        downstream: dict[str, set[str]] = {s.name: set() for s in stages}
        for st in stages:
            for dep in st.deps:
                found = tuple((o_stage, out) for o_stage, out in owners if overlaps(dep, out))
                if found:
                    producers[(st.name, dep)] = found
                for o_stage, _ in found:
                    upstream[st.name].add(o_stage)
                    downstream[o_stage].add(st.name)
        order = self._toposort(upstream, downstream)
        return Graph(
            order=tuple(order),
            upstream={k: frozenset(v) for k, v in upstream.items()},
            downstream={k: frozenset(v) for k, v in downstream.items()},
            producers=producers,
        )

    def _toposort(
        self, upstream: dict[str, set[str]], downstream: dict[str, set[str]]
    ) -> list[str]:
        """Kahn's algorithm; ties broken by declaration order for determinism."""
        index = {name: i for i, name in enumerate(self._stages)}
        indeg = {n: len(u) for n, u in upstream.items()}
        heap = [(index[n], n) for n, d in indeg.items() if d == 0]
        heapq.heapify(heap)
        order: list[str] = []
        while heap:
            _, n = heapq.heappop(heap)
            order.append(n)
            for m in downstream[n]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    heapq.heappush(heap, (index[m], m))
        if len(order) != len(index):
            remaining = {n for n in index if n not in order}
            raise PipelineError(
                f"dependency cycle: {' -> '.join(_find_cycle(remaining, upstream))}"
            )
        return order

    def upstream_closure(self, targets: Iterable[str]) -> set[str]:
        """``targets`` plus everything they transitively depend on."""
        graph = self.graph
        seen: set[str] = set()
        stack = [self.get(t).name for t in targets]
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(graph.upstream[n])
        return seen

    def downstream_closure(self, names: Iterable[str]) -> set[str]:
        """``names`` plus everything that transitively depends on them."""
        graph = self.graph
        seen: set[str] = set()
        stack = [self.get(t).name for t in names]
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(graph.downstream[n])
        return seen


def _find_cycle(nodes: set[str], upstream: dict[str, set[str]]) -> list[str]:
    """Return one cycle among ``nodes`` as a list ending where it started."""
    start = sorted(nodes)[0]
    path: list[str] = [start]
    pos = {start: 0}
    current = start
    while True:
        nxt = sorted(u for u in upstream[current] if u in nodes)[0]
        if nxt in pos:
            cycle = [*path[pos[nxt] :], nxt]
            cycle.reverse()  # present in producer -> consumer direction
            return cycle
        pos[nxt] = len(path)
        path.append(nxt)
        current = nxt
