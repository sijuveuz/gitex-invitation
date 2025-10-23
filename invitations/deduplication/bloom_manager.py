from pybloom_live import ScalableBloomFilter
from threading import Lock

class BloomManager:
    """
    Thread-safe Bloom filter manager.
    Keeps probabilistic record of seen items per namespace/job.
    """

    _filters = {}
    _lock = Lock()

    @classmethod
    def get_filter(cls, namespace="default"):
        with cls._lock:
            if namespace not in cls._filters:
                cls._filters[namespace] = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
            return cls._filters[namespace]

    @classmethod
    def seen_before(cls, namespace, item_key):
        bloom = cls.get_filter(namespace)
        if item_key in bloom:
            return True
        bloom.add(item_key)
        return False

    @classmethod
    def clear(cls, namespace):
        with cls._lock:
            cls._filters.pop(namespace, None)
