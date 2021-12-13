from .extension import Extension
from .converters.chunked_array import ChunkedArrayConverter

class CoreExtension(Extension):
    extension_uri = "asdf://asdf-format.org/core/extensions/core-1.7.0"

    @property
    def converters(self):
        return [
            ChunkedArrayConverter()
        ]

    @property
    def tags(self):
        return ["asdf://asdf-format.org/core/tags/chunked_ndarray-1.0.0"]


def get_core_extensions():
    return [
        CoreExtension()
    ]
