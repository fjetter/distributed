# mypy: ignore-errors
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import toolz

from dask.base import tokenize
from dask.core import keys_in_tasks
from dask.dataframe.core import Index, new_dd_object
from dask.dataframe.shuffle import partitioning_index
from dask.highlevelgraph import HighLevelGraph
from dask.layers import Layer
from dask.utils import stringify, stringify_collection_keys

from distributed.shuffle._shuffle import (
    ShuffleId,
    _get_worker_extension,
    barrier_key,
    shuffle_barrier,
    shuffle_transfer,
)

if TYPE_CHECKING:
    import pandas as pd


def hash_join_p2p(
    lhs,
    left_on,
    rhs,
    right_on,
    how="inner",
    npartitions=None,
    suffixes=("_x", "_y"),
    indicator=False,
):
    if npartitions is None:
        npartitions = max(lhs.npartitions, rhs.npartitions)

    if isinstance(left_on, Index):
        left_on = None
        left_index = True
    else:
        left_index = False

    if isinstance(right_on, Index):
        right_on = None
        right_index = True
    else:
        right_index = False
    # TODO: right/left_index still has to be forwarded
    merge_kwargs = dict(
        how=how,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        suffixes=suffixes,
        indicator=indicator,
    )
    # dummy result
    # Avoid using dummy data for a collection it is empty
    _lhs_meta = lhs._meta_nonempty if len(lhs.columns) else lhs._meta
    _rhs_meta = rhs._meta_nonempty if len(rhs.columns) else rhs._meta
    meta = _lhs_meta.merge(_rhs_meta, **merge_kwargs)
    merge_name = "hash-join-" + tokenize(lhs, left_on, lhs, right_on, suffixes)
    join_layer = HashJoinLayer(
        name=merge_name,
        name_input_left=lhs._name,
        left_on=left_on,
        name_input_right=rhs._name,
        right_on=right_on,
        meta_output=meta,
        how=how,
        npartitions=npartitions,
        suffixes=suffixes,
        indicator=indicator,
    )
    graph = HighLevelGraph.from_collections(
        merge_name, join_layer, dependencies=[lhs, rhs]
    )
    return new_dd_object(graph, merge_name, meta, [None] * (npartitions + 1))


hash_join = hash_join_p2p


def merge_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    input_partition: int,
    npartitions: int,
    column: str,
):
    hash_column = "__partition"
    input[hash_column] = partitioning_index(input[column], npartitions)
    return shuffle_transfer(
        input=input,
        id=id,
        input_partition=input_partition,
        npartitions=npartitions,
        column=hash_column,
    )


def merge_unpack(
    shuffle_id_left: ShuffleId,
    shuffle_id_right: ShuffleId,
    output_partition: int,
    barrier_left: int,
    barrier_right: int,
    result_meta,
):
    # FIXME: This is odd. There are similar things happening in dask/dask
    # layers.py but this works for now
    from dask.dataframe.multi import merge_chunk

    from distributed.protocol import deserialize

    result_meta = deserialize(result_meta.header, result_meta.frames)
    ext = _get_worker_extension()
    left = ext.get_output_partition(
        shuffle_id_left, barrier_left, output_partition
    ).drop(columns="__partition")
    right = ext.get_output_partition(
        shuffle_id_right, barrier_right, output_partition
    ).drop(columns="__partition")
    return merge_chunk(left, right, result_meta=result_meta)


class HashJoinLayer(Layer):
    def __init__(
        self,
        name: str,
        name_input_left: str,
        left_on,
        name_input_right: str,
        right_on,
        meta_output: pd.DataFrame,
        how="inner",
        npartitions=None,
        suffixes=("_x", "_y"),
        indicator=False,
        parts_out=None,
        annotations: dict | None = None,
    ) -> None:
        self.name = name
        self.name_input_left = name_input_left
        self.left_on = left_on
        self.name_input_right = name_input_right
        self.right_on = right_on
        self.how = how
        self.npartitions = npartitions
        self.suffixes = suffixes
        self.indicator = indicator
        self.meta_output = meta_output
        self.parts_out = parts_out or list(range(npartitions))
        annotations = annotations or {}
        # TODO: This is more complex
        annotations.update({"shuffle": lambda key: key[-1]})
        super().__init__(annotations=annotations)

    def _cull_dependencies(self, keys, parts_out=None):
        """Determine the necessary dependencies to produce `keys`.

        For a simple shuffle, output partitions always depend on
        all input partitions. This method does not require graph
        materialization.
        """
        # FIXME: I believe this is just wrong. For P2PLayer as well
        deps = defaultdict(set)
        parts_out = parts_out or self._keys_to_parts(keys)
        for part in parts_out:
            deps[(self.name, part)] |= {
                (self.name_input_left, i) for i in range(self.npartitions)
            }
            deps[(self.name, part)] |= {
                (self.name_input_right, i) for i in range(self.npartitions)
            }
        return deps

    def _keys_to_parts(self, keys):
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            try:
                _name, _part = key
            except ValueError:
                continue
            if _name != self.name:
                continue
            parts.add(_part)
        return parts

    def get_output_keys(self):
        return {(self.name, part) for part in self.parts_out}

    def __repr__(self):
        return f"HashJoin<name='{self.name}', npartitions={self.npartitions}>"

    def is_materialized(self):
        return hasattr(self, "_cached_dict")

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    @property
    def _dict(self):
        """Materialize full dict representation"""
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def _cull(self, parts_out):
        return HashJoinLayer(
            name=self.name,
            name_input_left=self.name_input_left,
            left_on=self.left_on,
            name_input_right=self.name_input_right,
            right_on=self.right_on,
            how=self.how,
            npartitions=self.npartitions,
            suffixes=self.suffixes,
            indicator=self.indicator,
            meta_output=self.meta_output,
            parts_out=parts_out,
            annotations=self.annotations,
        )

    def cull(self, keys, all_keys):
        """Cull a SimpleShuffleLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indices) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        culled_deps = self._cull_dependencies(keys, parts_out=parts_out)
        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def _construct_graph(self) -> dict[tuple | str, tuple]:
        token_left = tokenize(
            self.name_input_left, self.left_on, self.npartitions, self.parts_out
        )
        token_right = tokenize(
            self.name_input_right, self.right_on, self.npartitions, self.parts_out
        )
        dsk: dict[tuple | str, tuple] = {}
        # FIXME: This is a problem. The barrier key is parsed to infer the
        # shuffle ID that is currently used in the transition hook.
        # We should likely change this to use annotations instead which allows
        # for a richer embedding of information
        name_left = "hash-join-transfer-" + token_left
        name_right = "hash-join-transfer-" + token_right
        transfer_keys_left = list()
        transfer_keys_right = list()
        for i in range(self.npartitions):
            transfer_keys_left.append((name_left, i))
            dsk[(name_left, i)] = (
                merge_transfer,
                (self.name_input_left, i),
                token_left,
                i,
                self.npartitions,
                self.left_on,
            )
            transfer_keys_right.append((name_right, i))
            dsk[(name_right, i)] = (
                merge_transfer,
                (self.name_input_right, i),
                token_right,
                i,
                self.npartitions,
                self.right_on,
            )

        _barrier_key_left = barrier_key(ShuffleId(token_left))
        _barrier_key_right = barrier_key(ShuffleId(token_right))
        dsk[_barrier_key_left] = (shuffle_barrier, token_left, transfer_keys_left)
        dsk[_barrier_key_right] = (shuffle_barrier, token_right, transfer_keys_right)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (
                merge_unpack,
                token_left,
                token_right,
                part_out,
                _barrier_key_left,
                _barrier_key_right,
                self.meta_output,
            )
        return dsk

    @classmethod
    def __dask_distributed_unpack__(cls, state, dsk, dependecies):
        from distributed.worker import dumps_task

        to_list = [
            "left_on",
            "right_on",
        ]
        # msgpack will convert lists into tuples, here
        # we convert them back to lists
        for attr in to_list:
            if isinstance(state[attr], tuple):
                state[attr] = list(state[attr])

        # Materialize the layer
        layer_dsk = cls(**state)._construct_graph()

        # Convert all keys to strings and dump tasks
        layer_dsk = {
            stringify(k): stringify_collection_keys(v) for k, v in layer_dsk.items()
        }
        keys = layer_dsk.keys() | dsk.keys()
        # TODO: use shuffle-knowledge to calculate dependencies more efficiently
        deps = {k: keys_in_tasks(keys, [v]) for k, v in layer_dsk.items()}

        return {"dsk": toolz.valmap(dumps_task, layer_dsk), "deps": deps}

    def __dask_distributed_pack__(
        self, all_hlg_keys, known_key_dependencies, client, client_keys
    ):
        from distributed.protocol.serialize import to_serialize

        return {
            "name": self.name,
            "name_input_left": self.name_input_left,
            "left_on": self.left_on,
            "name_input_right": self.name_input_right,
            "right_on": self.right_on,
            "meta_output": to_serialize(self.meta_output),
            "how": self.how,
            "npartitions": self.npartitions,
            "suffixes": self.suffixes,
            "indicator": self.indicator,
            "parts_out": self.parts_out,
        }
