from __future__ import annotations

import pytest

from distributed.shuffle._merge import hash_join
from distributed.utils_test import gen_cluster

dd = pytest.importorskip("dask.dataframe")
import pandas as pd

from dask.dataframe._compat import tm
from dask.dataframe.utils import assert_eq
from dask.utils_test import hlg_layer_topological


def list_eq(aa, bb):
    if isinstance(aa, dd.DataFrame):
        a = aa.compute(scheduler="sync")
    else:
        a = aa
    if isinstance(bb, dd.DataFrame):
        b = bb.compute(scheduler="sync")
    else:
        b = bb
    tm.assert_index_equal(a.columns, b.columns)

    if isinstance(a, pd.DataFrame):
        av = a.sort_values(list(a.columns)).values
        bv = b.sort_values(list(b.columns)).values
    else:
        av = a.sort_values().values
        bv = b.sort_values().values

    dd._compat.assert_numpy_array_equal(av, bv)


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
@gen_cluster(client=True)
async def test_basic_merge(c, s, a, b, how):

    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    joined = hash_join(a, "y", b, "y", how)

    assert not hlg_layer_topological(joined.dask, -1).is_materialized()
    result = await c.compute(joined)
    expected = pd.merge(A, B, how, "y")
    list_eq(result, expected)

    # Different columns and npartitions
    joined = hash_join(a, "x", b, "z", "outer", npartitions=3)
    assert not hlg_layer_topological(joined.dask, -1).is_materialized()
    assert joined.npartitions == 3

    result = await c.compute(joined)
    expected = pd.merge(A, B, "outer", None, "x", "z")

    list_eq(result, expected)

    assert (
        hash_join(a, "y", b, "y", "inner")._name
        == hash_join(a, "y", b, "y", "inner")._name
    )
    assert (
        hash_join(a, "y", b, "y", "inner")._name
        != hash_join(a, "y", b, "y", "outer")._name
    )


@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
@gen_cluster(client=True)
async def test_merge(c, s, a, b, how):
    shuffle_method = "p2p"
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    res = await c.compute(
        dd.merge(
            a, b, left_index=True, right_index=True, how=how, shuffle=shuffle_method
        )
    )
    assert_eq(
        res,
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )
    joined = dd.merge(a, b, on="y", how=how)
    result = await c.compute(joined)
    list_eq(result, pd.merge(A, B, on="y", how=how))
    assert all(d is None for d in joined.divisions)

    list_eq(
        await c.compute(
            dd.merge(a, b, left_on="x", right_on="z", how=how, shuffle=shuffle_method)
        ),
        pd.merge(A, B, left_on="x", right_on="z", how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_on="x",
                right_on="z",
                how=how,
                suffixes=("1", "2"),
                shuffle=shuffle_method,
            )
        ),
        pd.merge(A, B, left_on="x", right_on="z", how=how, suffixes=("1", "2")),
    )

    list_eq(
        await c.compute(dd.merge(a, b, how=how, shuffle=shuffle_method)),
        pd.merge(A, B, how=how),
    )
    list_eq(
        await c.compute(dd.merge(a, B, how=how, shuffle=shuffle_method)),
        pd.merge(A, B, how=how),
    )
    list_eq(
        await c.compute(dd.merge(A, b, how=how, shuffle=shuffle_method)),
        pd.merge(A, B, how=how),
    )
    # Note: No await since A and B are both pandas dataframes and this doesn't
    # actually submit anything
    list_eq(
        c.compute(dd.merge(A, B, how=how, shuffle=shuffle_method)),
        pd.merge(A, B, how=how),
    )

    list_eq(
        await c.compute(
            dd.merge(
                a, b, left_index=True, right_index=True, how=how, shuffle=shuffle_method
            )
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_index=True,
                right_index=True,
                how=how,
                suffixes=("1", "2"),
                shuffle=shuffle_method,
            )
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how, suffixes=("1", "2")),
    )

    list_eq(
        await c.compute(
            dd.merge(
                a, b, left_on="x", right_index=True, how=how, shuffle=shuffle_method
            )
        ),
        pd.merge(A, B, left_on="x", right_index=True, how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_on="x",
                right_index=True,
                how=how,
                suffixes=("1", "2"),
                shuffle=shuffle_method,
            )
        ),
        pd.merge(A, B, left_on="x", right_index=True, how=how, suffixes=("1", "2")),
    )
