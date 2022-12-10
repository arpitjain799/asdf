class DeferredBlockSource:
    def __init__(self, key, data_callback, storage, compression, compression_kwargs, data_size=None):
        self._key = key
        self._data_callback = data_callback
        self._source = None
        self._storage = storage
        self._compression = compression
        self._compression_kwargs = compression_kwargs
        self._data_size = data_size

    @property
    def key(self):
        return self._key

    @property
    def data_callback(self):
        return self._data_callback

    @property
    def storage(self):
        return self._storage

    @property
    def compression(self):
        return self._compression

    @property
    def compression_kwargs(self):
        return self._compression_kwargs

    @property
    def source(self):
        if self._source is None:
            raise RuntimeError(f"Block with key {self.key} has not been assigned a source")
        return self._source

    @property
    def data_size(self):
        return self._data_size