from .bloom_manager import BloomManager
from .redis_deduper import RedisDeduper
from .utils import make_dedup_key

class DeduplicationService:
    """
    Unified deduplication service combining:
    - Bloom filter (fast pre-check)
    - Redis (atomic exact lock)
    """

    def __init__(self, namespace="default", ttl=3600):
        self.namespace = namespace
        self.ttl = ttl

    def is_duplicate(self, user_id, email, ticket_type):
        """
        Returns True if duplicate (exists before),
        False if new and locks it in Redis.
        """
        key = make_dedup_key(user_id, email, ticket_type)

        # Step 1: Bloom quick check
        seen_bloom = BloomManager.seen_before(self.namespace, key)
        if seen_bloom:
            # Might be true duplicate — confirm with Redis
            seen_redis = RedisDeduper.check_and_lock(key, ttl=self.ttl)
            if not seen_redis:
                BloomManager.add(self.namespace, key)  # ← Missing!
                return False
            return seen_redis

        # Step 2: New entry → set Redis lock
        RedisDeduper.check_and_lock(key, ttl=self.ttl)
        return False

    def clear(self):
        BloomManager.clear(self.namespace)
        RedisDeduper.clear_namespace(f"dedup:{self.namespace}")
