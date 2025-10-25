from .bloom_manager import BloomManager
from .redis_deduper import RedisDeduper
from .utils import make_dedup_key
import logging
from invitations.utils.redis_utils import get_redis
dedup_logger = logging.getLogger("django")

class DeduplicationService:
    def __init__(self, namespace="invite", ttl=3600):
        self.namespace = namespace
        self.ttl = ttl
        self.redis_client = get_redis()

    def is_duplicate(self, user_id, email, ticket_type):
        key = make_dedup_key(email, ticket_type)

        dedup_logger.debug(f"[DE-DUP] Checking key = {key}")

        # Step 1: Bloom quick check
        seen_bloom = BloomManager.seen_before(self.namespace, key)

        if seen_bloom:
            dedup_logger.debug(f"[DE-DUP] Bloom suggests: POSSIBLE DUPLICATE → {key}")

            # Confirm with Redis
            seen_redis = RedisDeduper.check_and_lock(key, ttl=self.ttl, redis_client = self.redis_client )

            if not seen_redis:
                dedup_logger.debug(f"[DE-DUP] Redis says: NOT duplicate → inserting into bloom → {key}")
                BloomManager.get_filter(self.namespace).add(key)
                return False

            dedup_logger.warning(f"[DE-DUP] ✅ CONFIRMED DUPLICATE → {key}")
            return True

        # Step 2: Bloom says new → lock in Redis
        first_time = RedisDeduper.check_and_lock(key, ttl=self.ttl, redis_client = self.redis_client )

        if not first_time:
            # Should not normally happen, but logged for clarity
            dedup_logger.warning(f"[DE-DUP] Redis LOCK existed but bloom didn't know → {key}")

        dedup_logger.info(f"[DE-DUP] ✅ NEW ENTRY (first time) → key stored → {key}")
        return False
