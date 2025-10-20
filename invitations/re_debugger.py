import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gitex_invitation.settings')

import django
django.setup()

from invitations.utils.redis_utils import get_redis
import orjson
from collections import Counter

def find_duplicate_rows(job_id, field="row_number"):
    r = get_redis()
    key = f"bulk:job:{job_id}:rows"
    rows_raw = r.lrange(key, 0, -1)

    rows = [orjson.loads(row) for row in rows_raw]

    values = []
    for row in rows:
        val = row.get(field)
        if val is not None:
            value = str(val).strip().lower()
            values.append(value)

    counter = Counter(values)
    duplicates = {v: c for v, c in counter.items() if c > 1}

    print(f"Total rows: {len(values)}")
    print(f"Duplicates found: {len(duplicates)}")
    for v, c in duplicates.items():
        print(f"  {field}={v} → {c} occurrences")

if __name__ == "__main__":
    find_duplicate_rows("bb4157cd-c136-47d5-8bf5-651dafd39b82", field="row_number")


