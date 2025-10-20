import orjson
from django.conf import settings
import redis

REDIS_URL = getattr(settings, "REDIS_URL", "redis://127.0.0.1:6379/0")
_redis = None

def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis

def push_row(job_id, row_obj):
    """Store a row in Redis Hash with id as key."""
    r = get_redis()
    key = f"bulk:job:{job_id}:rows"
    row_id = row_obj["id"]
    r.hset(key, row_id, orjson.dumps(row_obj))

def range_rows(job_id, id_list=None):
    """Fetch specific rows by id or all rows."""
    r = get_redis()
    key = f"bulk:job:{job_id}:rows"
    if id_list:
        rows = [orjson.loads(r.hget(key, str(id))) for id in id_list if r.hexists(key, str(id))]
    else:
        rows = [orjson.loads(v) for v in r.hgetall(key).values()]
    return sorted(rows, key=lambda r: r["id"])  # Sort by id for consistency

def update_row(job_id, row_id, row_obj):
    """Update a row in Redis Hash by id."""
    print(f"UPDATING ROW IN REDIS: id={row_id}, data={row_obj}")
    r = get_redis()
    key = f"bulk:job:{job_id}:rows"
    r.hset(key, row_id, orjson.dumps(row_obj))

def delete_row(job_id, row_id):
    """Delete a row from Redis Hash by id."""
    r = get_redis()
    key = f"bulk:job:{job_id}:rows"
    r.hdel(key, str(row_id))

def delete_rows_key(job_id):
    """Delete entire rows Hash."""
    r = get_redis()
    r.delete(f"bulk:job:{job_id}:rows")

def set_stats(job_id, **kwargs):
    """Set job stats."""
    r = get_redis()
    key = f"bulk:job:{job_id}:stats"
    if kwargs:
        r.hset(key, mapping={k: str(v) for k, v in kwargs.items()})

def incr_stats(job_id, field, amount=1):
    """Increment a stat field."""
    r = get_redis()
    key = f"bulk:job:{job_id}:stats"
    r.hincrby(key, field, amount)

def get_stats(job_id):
    """Get job stats, converting numeric strings to ints."""
    r = get_redis()
    key = f"bulk:job:{job_id}:stats"
    data = r.hgetall(key)
    result = {}
    for k, v in data.items():
        if v is None:
            result[k] = v
        elif str(v).isdigit():
            result[k] = int(v)
        else:
            result[k] = v
    return result

def set_status(job_id, status):
    """Set job status."""
    r = get_redis()
    r.set(f"bulk:job:{job_id}:status", status)

def get_status(job_id):
    """Get job status."""
    r = get_redis()
    return r.get(f"bulk:job:{job_id}:status")

def set_export_progress(job_id, processed, total):
    """Set export progress."""
    r = get_redis()
    key = f"export:job:{job_id}:progress"
    r.hset(key, mapping={"processed": str(processed), "total": str(total)})
    r.expire(key, 60*60*24)

def get_export_progress(job_id):
    """Get export progress."""
    r = get_redis()
    key = f"export:job:{job_id}:progress"
    data = r.hgetall(key)
    if not data:
        return {}
    return {k: int(v) if str(v).isdigit() else v for k, v in data.items()}



# import json
# import orjson
# from django.conf import settings
# import redis

# REDIS_URL = getattr(settings, "REDIS_URL", "redis://127.0.0.1:6379/0")
# _redis = None

# def get_redis():
#     global _redis
#     if _redis is None:
#         _redis = redis.from_url(REDIS_URL, decode_responses=True)
#     return _redis

# # In push_row, update_row, etc.:
# def push_row(job_id, row_obj):
#     r = get_redis()
#     r.rpush(f"bulk:job:{job_id}:rows", orjson.dumps(row_obj))  # bytes OK with decode_responses

# # In range_rows:
# def range_rows(job_id, start, end):
#     r = get_redis()
#     items = r.lrange(f"bulk:job:{job_id}:rows", start, end)
#     return [orjson.loads(i) for i in items]  # orjson.loads handles str/bytes

# def update_row(job_id, index_zero, row_obj):
#     print("UPDATING ROW IN REDIS :", index_zero, row_obj)
#     r = get_redis()
#     r.lset(f"bulk:job:{job_id}:rows", index_zero, orjson.dumps(row_obj))

# def delete_rows_key(job_id):
#     r = get_redis()
#     r.delete(f"bulk:job:{job_id}:rows")

# def set_stats(job_id, **kwargs):
#     """
#     Return the stats of the job
#     """
#     r = get_redis()
#     key = f"bulk:job:{job_id}:stats"
#     if kwargs:
#         r.hset(key, mapping={k: str(v) for k, v in kwargs.items()})

# def incr_stats(job_id, field, amount=1):
#     r = get_redis()
#     key = f"bulk:job:{job_id}:stats"
#     r.hincrby(key, field, amount)

# def get_stats(job_id):
#     """
#     The overall status like valid , invalid , total """
#     r = get_redis()
#     key = f"bulk:job:{job_id}:stats"
#     data = r.hgetall(key)
#     # convert numeric strings to ints when possible
#     result = {}
#     for k, v in data.items():
#         if v is None:
#             result[k] = v
#         else:
#             if str(v).isdigit():
#                 result[k] = int(v)
#             else:
#                 result[k] = v
#     return result

# def set_status(job_id, status):
#     r = get_redis()
#     r.set(f"bulk:job:{job_id}:status", status)

# def get_status(job_id):
#     r = get_redis()
#     return r.get(f"bulk:job:{job_id}:status")

# def set_export_progress(job_id, processed, total):
#     r = get_redis()
#     key = f"export:job:{job_id}:progress"
#     r.hset(key, mapping={"processed": str(processed), "total": str(total)})
#     # optional TTL to auto-expire
#     r.expire(key, 60*60*24)

# def get_export_progress(job_id):
#     r = get_redis()
#     key = f"export:job:{job_id}:progress"
#     data = r.hgetall(key)
#     if not data:
#         return {}
#     return {k: int(v) if str(v).isdigit() else v for k, v in data.items()}