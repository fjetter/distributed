from __future__ import annotations

import textwrap
from collections import defaultdict
from typing import Any

from tlz import valmap

import dask
from dask.core import get_deps
from dask.utils import stringify

from distributed.protocol import deserialize
from distributed.utils_comm import unpack_remotedata
from distributed.worker import dumps_task


def materialize_graph(
    graph_header: dict, graph_frames: list[bytes], global_annotations: dict
) -> tuple[dict, dict, dict]:
    try:
        graph = deserialize(graph_header, graph_frames).data
        del graph_header, graph_frames
    except Exception as e:
        msg = """\
            Error during deserialization of the task graph. This frequently occurs if the Scheduler and Client have different environments. For more information, see https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments
        """
        raise RuntimeError(textwrap.dedent(msg)) from e

    dsk = dask.utils.ensure_dict(graph)

    annotations_by_type: defaultdict[str, dict[str, Any]] = defaultdict(dict)
    for annotations_type, value in global_annotations.items():
        annotations_by_type[annotations_type].update(
            {k: (value(k) if callable(value) else value) for k in dsk}
        )

    for layer in graph.layers.values():
        if layer.annotations:
            annot = layer.annotations
            for annot_type, value in annot.items():
                annotations_by_type[annot_type].update(
                    {
                        stringify(k): (value(k) if callable(value) else value)
                        for k in layer
                    }
                )

    dependencies, _ = get_deps(dsk)

    # Remove `Future` objects from graph and note any future dependencies
    dsk2 = {}
    fut_deps = {}
    for k, v in dsk.items():
        dsk2[k], futs = unpack_remotedata(v, byte_keys=True)
        if futs:
            fut_deps[k] = futs
    dsk = dsk2

    # - Add in deps for any tasks that depend on futures
    for k, futures in fut_deps.items():
        dependencies[k].update(f.key for f in futures)
    new_dsk = {}
    # Annotation callables are evaluated on the non-stringified version of
    # the keys
    exclusive = set(graph)
    for k, v in dsk.items():
        new_k = stringify(k)
        new_dsk[new_k] = stringify(v, exclusive=exclusive)
    dsk = new_dsk
    dependencies = {
        stringify(k): {stringify(dep) for dep in deps}
        for k, deps in dependencies.items()
    }

    # Remove any self-dependencies (happens on test_publish_bag() and others)
    for k, v in dependencies.items():
        deps = set(v)
        if k in deps:
            deps.remove(k)
        dependencies[k] = deps

    # Remove aliases
    for k in list(dsk):
        if dsk[k] is k:
            del dsk[k]
    dsk = valmap(dumps_task, dsk)

    return dsk, dependencies, annotations_by_type


def materialize_maybe_order(
    graph_header: dict,
    graph_frames: list[bytes],
    annotations: dict,
    order: bool,
) -> tuple[dict, dict, dict, dict]:
    (
        dsk,
        dependencies,
        annotations_by_type,
    ) = materialize_graph(graph_header, graph_frames, annotations)
    del graph_header, graph_frames

    ordered: dict = {}
    if order:
        # Removing all non-local keys before calling order()
        dsk_keys = set(dsk)  # intersection() of sets is much faster than dict_keys
        stripped_deps = {
            k: v.intersection(dsk_keys)
            for k, v in dependencies.items()
            if k in dsk_keys
        }
        ordered = dask.order.order(dsk, dependencies=stripped_deps)

    return dsk, dependencies, annotations_by_type, ordered
