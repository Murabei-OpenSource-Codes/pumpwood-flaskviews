"""Module to define in call cache using g object."""
import copy
from flask import g
from typing import Any
from loguru import logger
from pumpwood_communication.cache import default_cache


class PumpwoodFlaskGCache:
    """Class to help set and retrieve cached data from requests."""

    CACHE_DICT_ATT: str = "pumpwood_cache"
    """Name of the attribute at the g object to cache information in
       request."""

    @classmethod
    def get_cache_dict(cls) -> dict:
        """Get the cache dict at the g object.

        Args:
            No args.

        Returns:
            Return the dictionary for caching data at g object.
        """
        if not hasattr(g, cls.CACHE_DICT_ATT):
            setattr(g, cls.CACHE_DICT_ATT, {})
        return getattr(g, cls.CACHE_DICT_ATT)

    @classmethod
    def generate_hash(cls, hash_dict: dict) -> str:
        """Generate hash for cache using communication generate_hash.

        Args:
            hash_dict (dict):
                A dictonary with information that will be used on hash.

        Returns:
            Return a hash that will be used as cache.
        """
        return default_cache.generate_hash(hash_dict=hash_dict)

    @classmethod
    def get(cls, hash_dict: dict, copy_value: bool = False) -> Any:
        """Get a value from cache.

        Args:
            hash_dict (dict):
                A dictonary with information that will be used on hash.
            copy_value (bool):
                Use deep copy to copy the content of the cached value to
                return the possibility of changes on complex variables like
                list and dict has not desired behavior due to memory
                reference.

        Returns:
            Return the cached value or None if not found.
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
        """Set cache value.

        Args:
            hash_dict (dict):
                A dictonary with information that will be used on hash.
            value (Any):
                Value that will be set on diskcache.

        Returns:
            Return a boolean value
        """
        hash_str = cls.generate_hash(hash_dict=hash_dict)
        cache_dict = cls.get_cache_dict()
        cache_dict[hash_str] = value
        return True


class PumpwoodFlaskGDiskCache:
    """Create a double layer of caching using in request g and diskcache."""

    @classmethod
    def get(cls, hash_dict: dict, copy_value: bool = False) -> Any:
        """Get a value from cache.

        Args:
            hash_dict (dict):
                A dictonary with information that will be used on hash.
            copy_value (bool):
                Use deep copy to copy the content of the cached value to
                return the possibility of changes on complex variables like
                list and dict has not desired behavior due to memory
                reference.

        Returns:
            Return the cached value or None if not found.
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
        """Set cache value.

        Args:
            hash_dict (dict):
                A dictonary with information that will be used on hash.
            value (Any):
                Value that will be set on diskcache.
            expire (int):
                Number of seconds that will be considered as expirity time.
            tag_dict (dict):
                Optional parameter to set a tag to cache. Tagged cache can be
                envicted together using envict function.

        Returns:
            Return a boolean value
        """
        PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=value)
        default_cache.set(
            hash_dict=hash_dict, value=value, expire=expire,
            tag_dict=tag_dict)
        return True
