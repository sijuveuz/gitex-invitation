import redis
from decouple import config

def get_redis():
    return redis.Redis.from_url(config("REDIS_URL", default="redis://localhost:6379/0"), decode_responses=True)

class RedisDeduper:
    """
    Uses Redis SETNX or SADD for exact duplicate prevention.
    """

    @staticmethod
    def check_and_lock(key, ttl=3600, redis_client= "redis://localhost:6379/0"):
        r = redis_client
        # SETNX (set if not exists)
        was_set = r.setnx(key, 1)
        if was_set:
            r.expire(key, ttl)
            return False  # Not duplicate
        return True  # Duplicate already seen

    @staticmethod
    def clear_namespace(namespace_prefix, redis_client):
        r = redis_client
        keys = r.keys(f"{namespace_prefix}*")
        if keys:
            r.delete(*keys)
