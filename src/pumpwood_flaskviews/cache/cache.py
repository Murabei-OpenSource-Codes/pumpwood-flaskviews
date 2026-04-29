"""Module to define in call cache using g object."""
import copy
from flask import g
from typing import Any
from loguru import logger
from pumpwood_communication.cache import default_cache


class PumpwoodFlaskGCache:
    """Helper to manage request-scoped caching using the Flask `g` object.

    Provides a simple key-value store tied to the lifecycle of a single
    HTTP request.
    """

    CACHE_DICT_ATT: str = "pumpwood_cache"
    """Name of the attribute at the g object to cache information in
       request."""

    @classmethod
    def get_cache_dict(cls) -> dict:
        """Retrieve or initialize the cache dictionary within the `g` object.

        Returns:
            dict:
                The dictionary stored in `g.pumpwood_cache`.
        """
        if not hasattr(g, cls.CACHE_DICT_ATT):
            setattr(g, cls.CACHE_DICT_ATT, {})
        return getattr(g, cls.CACHE_DICT_ATT)

    @classmethod
    def generate_hash(cls, hash_dict: dict) -> str:
        """Generate a consistent hash string from a dictionary.

        Args:
            hash_dict (dict):
                A dictionary containing the key components for hashing.

        Returns:
            str:
                A deterministic hash string.
        """
        return default_cache.generate_hash(hash_dict=hash_dict)

    @classmethod
    def get(cls, hash_dict: dict, copy_value: bool = False) -> Any:
        """Retrieve a value from the request-scoped cache.

        Args:
            hash_dict (dict):
                A dictionary representing the cache key.
            copy_value (bool):
                If True, returns a deepcopy of the cached data. Use this
                to prevent modification of the shared cache state by
                subsequent logic.

        Returns:
            Any:
                The cached value or None if not found.
        """
        hash_str = cls.generate_hash(hash_dict=hash_dict)
        cache_dict = cls.get_cache_dict()
        cached_data = cache_dict.get(hash_str)

        if cached_data is not None:
            logger.info("Cache retrieved from g object")
            # Implement the copy logic we discussed:
            # If copy_value is True, we return a deepcopy to avoid
            # modifying the cached version in memory.
            if copy_value:
                return copy.deepcopy(cached_data)
            else:
                return cached_data
        else:
            return cached_data

    @classmethod
    def set(cls, hash_dict: dict, value: Any) -> bool:
        """Store a value in the request-scoped cache.

        Args:
            hash_dict (dict):
                A dictionary representing the cache key.
            value (Any):
                The value to be cached.

        Returns:
            bool:
                Always returns True.
        """
        hash_str = cls.generate_hash(hash_dict=hash_dict)
        cache_dict = cls.get_cache_dict()
        cache_dict[hash_str] = value
        return True


class PumpwoodFlaskGDiskCache:
    """A dual-layer cache leveraging both `g` (RAM) and `DiskCache`.

    Prioritizes fast request-scoped memory before falling back to
    persistent disk storage.
    """

    @classmethod
    def get(cls, hash_dict: dict, copy_value: bool = False) -> Any:
        """Retrieve a value from the multi-layer cache.

        Checks the request-scoped `g` object first, then queries the
        configured DiskCache.

        Args:
            hash_dict (dict):
                A dictionary representing the cache key.
            copy_value (bool):
                If True, returns a deepcopy of the result to prevent
                in-memory mutation.

        Returns:
            Any:
                The cached value or None if missing from both layers.
        """
        g_cached_data = PumpwoodFlaskGCache.get(
            hash_dict=hash_dict, copy_value=copy_value)
        if g_cached_data is not None:
            return g_cached_data

        disk_cached_data = default_cache.get(hash_dict=hash_dict)

        # If disk cache was found, set it on g object cache to reduce the calls
        # on the disk cache.
        if disk_cached_data is not None:
            PumpwoodFlaskGCache.set(
                hash_dict=hash_dict, value=disk_cached_data)
        return disk_cached_data

    @classmethod
    def set(cls, hash_dict: dict, value: Any, expire: int = None,
            tag_dict: dict = None) -> bool:
        """Store a value in both the request-scoped and global disk caches.

        Args:
            hash_dict (dict):
                A dictionary representing the cache key.
            value (Any):
                The value to store.
            expire (int):
                Seconds until the global cache entry expires.
            tag_dict (dict):
                Metadata tags used for bulk eviction.

        Returns:
            bool:
                Always returns True.
        """
        PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=value)
        default_cache.set(
            hash_dict=hash_dict, value=value, expire=expire,
            tag_dict=tag_dict)
        return True
