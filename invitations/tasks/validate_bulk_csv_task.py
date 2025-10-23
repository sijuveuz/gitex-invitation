import csv
# import json
from celery import shared_task, current_task
from adminapp.models import TicketType
from invitations.models import BulkUploadJob, Invitation, InvitationStats
from invitations.utils.redis_utils import push_row, incr_stats, set_stats, set_status, delete_rows_key, get_redis 
# from invitations.helpers.bulk_helpers.bulk_validator import validate_row_csv_dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import csv


import csv
from django.core.files.storage import default_storage



PREVIEW_LIMIT = 50
BATCH_SIZE = 5000  # batch Redis operations every N rows
BATCH_CREATE = 5000 

MAX_THREADS = 15  # adjust per CPU cores
import orjson
import math
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from django.core.files.storage import default_storage
from celery import shared_task
from invitations.models import BulkUploadJob, Invitation
from invitations.utils.redis_utils import (
    get_redis, delete_rows_key, get_stats, set_stats, set_status, push_row
)
from invitations.helpers.bulk_helpers.bulk_validator import (
    load_ticket_types_cache, clear_ticket_types_cache, validate_row_csv_dict
) 
from decouple import config
from ..utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context


BATCH_SIZE = config("BULK_BATCH_SIZE", cast=int, default=1000)
MAX_THREADS = config("BULK_MAX_THREADS", cast=int, default=4)
PREVIEW_LIMIT = config("BULK_PREVIEW_LIMIT", cast=int, default=5)




@shared_task(bind=True)
def validate_csv_file_task(self, job_id, default_message=None):
    """
    Optimized bulk CSV validator.
    - Loads ticket types + existing invites once
    - Uses Redis cache for ticket list
    - Thread-safe file-level duplicate tracking
    - Respects global/ticket-level uniqueness rules
    """
    job = BulkUploadJob.objects.get(id=job_id)
    job.status = BulkUploadJob.STATUS_PROCESSING
    job.save(update_fields=["status"])

    # --- Redis setup ---
    set_stats(job_id, total_count=0, valid_count=0, invalid_count=0)
    set_status(job_id, "processing")
    delete_rows_key(job_id)
    r = get_redis()
    pipe = r.pipeline(transaction=False)

    # --- Preload all reusable data (1x DB hits only) ---
    ticket_cache = {
        t.name.lower(): t
        for t in TicketType.objects.filter(is_active=True)
    }

    existing_raw = Invitation.objects.filter(user=job.user).values_list(
        "guest_email", "ticket_type__name"
    )

    # global & ticket-level caches
    existing_global = {email.lower() for email, _ in existing_raw if email}
    existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}

    # file-level duplicate trackers (thread-safe)
    seen_global_dupes = {}
    seen_ticket_dupes = {}
    seen_lock = Lock()

    # --- Read uploaded CSV ---
    file_path = job.uploaded_file.name
    with default_storage.open(file_path, "rb") as fh:
        wrapper = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        reader = list(csv.DictReader(wrapper))

    total_rows = len(reader)
    chunk_count = math.ceil(total_rows / BATCH_SIZE)

    valid = invalid = 0
    preview = []

    global_unique_enabled, ticket_cache = load_ticket_email_validation_context()
    def process_rows(rows, start_index):
        results = []
        for i, row in enumerate(rows, start=start_index):
            row_obj, _ = validate_row_csv_dict(
                row=row,
                row_number=i,
                user=job.user,
                existing_global=existing_global,
                existing_ticket=existing_ticket,
                ticket_cache=ticket_cache,
                global_unique_enabled = global_unique_enabled,
                seen_global_dupes=seen_global_dupes,
                seen_ticket_dupes=seen_ticket_dupes,
                seen_lock=seen_lock,
                default_message=default_message,

            )
            row_obj["id"] = i
            results.append(row_obj)
        return results

    # --- Parallel batch validation ---
    for chunk_index in range(chunk_count):
        start = chunk_index * BATCH_SIZE + 1
        chunk = reader[start - 1:start - 1 + BATCH_SIZE]
        results = []

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            sub_size = max(1, len(chunk) // MAX_THREADS)
            for t in range(0, len(chunk), sub_size):
                part = chunk[t:t + sub_size]
                futures.append(executor.submit(process_rows, part, start + t))

            for f in as_completed(futures):
                results.extend(f.result())

        batch_valid = batch_invalid = 0
        for row_obj in results:
            pipe.hset(f"bulk:job:{job_id}:rows", row_obj["id"], orjson.dumps(row_obj))
            if row_obj["status"] == "valid":
                valid += 1
                batch_valid += 1
            else:
                invalid += 1
                batch_invalid += 1
                if len(preview) < PREVIEW_LIMIT:
                    preview.append(row_obj)

        pipe.hincrby(f"bulk:job:{job_id}:stats", "total_count", len(results))
        pipe.hincrby(f"bulk:job:{job_id}:stats", "valid_count", batch_valid)
        pipe.hincrby(f"bulk:job:{job_id}:stats", "invalid_count", batch_invalid)
        pipe.execute()

        # update Celery task progress
        self.update_state(
            state="PROGRESS",
            meta={"processed": start + len(results) - 1, "total": total_rows},
        )

    # --- Finalize job ---
    existing_global.clear()
    existing_ticket.clear()
    ticket_cache.clear()

    job.total_count = total_rows
    job.valid_count = valid
    job.invalid_count = invalid
    job.preview_data = preview[:PREVIEW_LIMIT]
    job.status = BulkUploadJob.STATUS_PREVIEW_READY
    job.save(update_fields=[
        "total_count", "valid_count", "invalid_count",
        "preview_data", "status", "updated_at"
    ])

    set_status(job_id, "done")

    return {
        "job_id": str(job.id),
        "total": total_rows,
        "valid": valid,
        "invalid": invalid,
    }



# @shared_task(bind=True)
# def validate_csv_file_task(self, job_id, default_message=None):
#     job = BulkUploadJob.objects.get(id=job_id)
#     job.status = BulkUploadJob.STATUS_PROCESSING
#     job.save(update_fields=["status"])

#     set_stats(job_id, total_count=0, valid_count=0, invalid_count=0)
#     set_status(job_id, "processing")

#     load_ticket_types_cache()

#     existing_invites_raw = Invitation.objects.filter(user=job.user).values_list(
#         "guest_email", "ticket_type__name"
#     )
#     existing_invites = {
#         ((e or "").lower(), (t or "").lower())
#         for e, t in existing_invites_raw
#         if e and t
#     }

#     total = valid = invalid = 0
#     preview = []
#     delete_rows_key(job_id)

#     r = get_redis()
#     pipe = r.pipeline(transaction=False)

#     file_path = job.uploaded_file.name
#     with default_storage.open(file_path, "rb") as fh:
#         wrapper = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
#         reader = list(csv.DictReader(wrapper))

#     total_rows = len(reader)
#     chunk_count = math.ceil(total_rows / BATCH_SIZE)
#     seen_file_duplicates = {}
#     seen_lock = Lock()

#     def process_rows(rows, start_index):
#         results = []
#         for i, row in enumerate(rows, start=start_index):
#             row_obj, _ = validate_row_csv_dict(
#                 row,
#                 i,  # row_number for user-facing errors
#                 default_message=default_message,
#                 existing_invites=existing_invites,
#                 seen_file_duplicates=seen_file_duplicates,
#                 seen_lock=seen_lock,
#             )
#             row_obj["id"] = i  # Assign id (same as row_number for initial upload)
#             results.append(row_obj)
#         return results

#     for chunk_index in range(chunk_count):
#         start = chunk_index * BATCH_SIZE + 1  # 1-based for id/row_number
#         chunk = reader[start-1:start-1 + BATCH_SIZE]
#         results = []

#         with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
#             futures = []
#             sub_size = max(1, len(chunk) // MAX_THREADS)
#             for t in range(0, len(chunk), sub_size):
#                 part = chunk[t:t + sub_size]
#                 futures.append(executor.submit(process_rows, part, start + t))

#             for f in as_completed(futures):
#                 results.extend(f.result())

#         batch_valid = batch_invalid = 0
#         for row_obj in results:
#             pipe.hset(f"bulk:job:{job_id}:rows", row_obj["id"], orjson.dumps(row_obj))
#             if row_obj["status"] == "valid":
#                 valid += 1
#                 batch_valid += 1
#             else:
#                 invalid += 1
#                 batch_invalid += 1
#                 if len(preview) < PREVIEW_LIMIT:
#                     preview.append(row_obj)

#         pipe.hincrby(f"bulk:job:{job_id}:stats", "total_count", len(results))
#         pipe.hincrby(f"bulk:job:{job_id}:stats", "valid_count", batch_valid)
#         pipe.hincrby(f"bulk:job:{job_id}:stats", "invalid_count", batch_invalid)
#         pipe.execute()

#         self.update_state(
#             state="PROGRESS",
#             meta={"processed": start + len(results) - 1, "total": total_rows},
#         )

#     clear_ticket_types_cache()
#     existing_invites.clear()

#     total = total_rows
#     job.total_count = total
#     job.valid_count = valid
#     job.invalid_count = invalid
#     job.preview_data = preview[:PREVIEW_LIMIT]
#     job.status = BulkUploadJob.STATUS_PREVIEW_READY
#     job.save(update_fields=[
#         "total_count", "valid_count", "invalid_count",
#         "preview_data", "status", "updated_at"
#     ])

#     set_status(job_id, "done")

#     return {
#         "job_id": str(job.id),
#         "total": total,
#         "valid": valid,
#         "invalid": invalid,
#     }
