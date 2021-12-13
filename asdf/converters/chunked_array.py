import json
import math

from collections.abc import MutableMapping

from ..extension import Converter
from ..tags.core import ndarray
import numpy as np


def _get_at_index(sequence, key):
    for idx in [int(k) for k in key.split(".")]:
        sequence = sequence[idx]
    return sequence


def _set_at_index(sequence, key, value):
    indices = [int(k) for k in key.split(".")]
    for idx in indices[:-1]:
        sequence = sequence[idx]
    sequence[indices[-1]] = value


def _create_sources(shape, chunk_shape):
    num_chunks = math.ceil(shape[0] / chunk_shape[0])
    if len(shape) == 1:
        return [None] * num_chunks
    else:
        result = []
        for _ in range(num_chunks):
            result.append(_create_sources(shape[1:], chunk_shape[1:]))
        return result


def _create_data_callback(obj, array_key):
    # For now, we only support C-contiguous arrays
    return lambda: np.ascontiguousarray(obj[tuple(array_key)])


class ChunkedArrayConverter(Converter):
    tags = ["asdf://asdf-format.org/core/tags/chunked_ndarray-*"]
    types = ["zarr.core.Array"]

    def to_yaml_tree(self, obj, tag, ctx):
        shape = list(obj.shape)
        chunk_shape = list(obj.chunks)
        datatype, byteorder = ndarray.numpy_dtype_to_asdf_datatype(obj.dtype)

        sources = _create_sources(shape, chunk_shape)

        for key in obj.store.keys():
            if key != ".zarray":
                indices = [int(k) for k in key.split(".")]
                array_key = []
                for i, idx in enumerate(indices):
                    array_key.append(slice(idx * chunk_shape[i], (idx + 1) * chunk_shape[i]))
                block = ctx.block_manager.find_or_create_block_for_array(_create_data_callback(obj, array_key), None, key=(id(obj), key))
                _set_at_index(sources, key, ctx.block_manager.get_source(block))

        return {
            "shape": shape,
            "chunk_shape": chunk_shape,
            "datatype": datatype,
            "byteorder": byteorder,
            "sources": sources
        }

    def from_yaml_tree(self, node, tag, ctx):
        from zarr.core import Array

        storage = AsdfStorage(
            node["shape"],
            node["chunk_shape"],
            ndarray.asdf_datatype_to_numpy_dtype(node["datatype"], node["byteorder"]),
            node["sources"],
            ctx.block_manager,
        )

        return Array(storage)


class AsdfStorage(MutableMapping):
    def __init__(self, shape, chunk_shape, dtype, sources, block_manager):
        self._config = {
            "zarr_format": 2,
            "shape": shape,
            "chunks": chunk_shape,
            "dtype": str(dtype),
            "compressor": None,
            "fill_value": None,
            "order": "C",
            "filters": None,
        }

        self._sources = sources

        self._block_manager = block_manager

    def __getitem__(self, key):
        if key == ".zarray":
            return json.dumps(self._config)

        source = _get_at_index(self._sources, key)

        if source is None:
            raise KeyError(key)

        return self._block_manager.get_block(source).data.tobytes()

    def __setitem__(self, key, value):
        if key == ".zarray":
            raise ValueError("Cannot set zarr config on AsdfStorage class")

        source = _get_at_index(self._sources, key)
        if source is None:
            pass
        else:
            block = self._block_manager.get_block(source)
            block.data[:] = np.ravel(value, order="C").view(np.uint8)
    #    self._block_manager.find_or_create_block_for_array(_create_data_callback(obj, array_key), None, key=(id(obj), key))

    def __delitem__(self, key):
        if key == ".zarray":
            raise ValueError("Cannot delete zarr config on AsdfStorage class")

        print(key)

        raise NotImplementedError("AsdfStorage is currently read-only")

    def __iter__(self):
        raise NotImplementedError("AsdfStorage cannot be iterated")

    def __len__(self):
        raise NotImplementedError("AsdfStorage has no length")
