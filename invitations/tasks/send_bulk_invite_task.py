from celery import shared_task
from invitations.models import BulkUploadJob, Invitation, InvitationStats
from adminapp.models import TicketType, DuplicateRecord
from invitations.utils.redis_utils import get_redis, delete_rows_key
from invitations.deduplication.dedup_service import DeduplicationService
from invitations.deduplication.utils import resolve_dedup_scope
from ..utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context
from decouple import config
import orjson
import uuid

BATCH_CREATE = 5000  # Batch size for creating invitations


# ==============================
# 🔹 HELPER FUNCTIONS
# ==============================

def get_bulk_job_and_setup(job_id):
    """Fetch BulkUploadJob, set status, and prepare dedup service + Redis."""
    job = BulkUploadJob.objects.get(id=job_id)
    job.status = BulkUploadJob.STATUS_SENDING
    job.save(update_fields=["status"])
    dedup = DeduplicationService(namespace=f"bulk:{job_id}")
    redis_client = get_redis()
    return job, dedup, redis_client


def fetch_rows_from_redis(redis_client, job_id):
    """Fetch and parse all valid rows from Redis for a given job."""
    key = f"bulk:job:{job_id}:rows"
    rows_dict = redis_client.hgetall(key)
    return [orjson.loads(value) for value in rows_dict.values()], key


def prepare_invitation_data(job):
    """Precompute all necessary context for invitation creation."""
    BASE_URL = config("FRONTEND_INVITE_URL", "http://178.18.253.63:3010/invite/")
    ticket_map = {t.name.lower(): t.id for t in TicketType.objects.filter(is_active=True)}
    stats, _ = InvitationStats.objects.get_or_create(user=job.user)

    existing_raw = Invitation.objects.filter(user=job.user).values_list(
        "guest_email", "ticket_type__name"
    )
    existing_global = {email.lower() for email, _ in existing_raw if email}
    existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}

    global_unique_enabled, ticket_cache = load_ticket_email_validation_context()
    return BASE_URL, ticket_map, stats, existing_global, existing_ticket, global_unique_enabled, ticket_cache


def handle_deduplication(job, dedup, email, ticket_name, scope):
    """Check and log duplicates using the dedup service."""
    is_dup = False
    if scope != "none":
        is_dup = dedup.is_duplicate(
            user_id=job.user.id,
            email=email,
            ticket_type=ticket_name if scope == "ticket" else "",
        )
        if is_dup:
            DuplicateRecord.objects.create(
                user=job.user,
                job=job,
                guest_email=email,
                ticket_type=TicketType.objects.filter(name__iexact=ticket_name).first(),
                detection_source="dedup_service",
                scope=scope,
                reason=f"Duplicate {scope} for {email} ({ticket_name}) detected by dedup service",
            )
    return is_dup


def create_invitation_objects(chunk, job, BASE_URL, ticket_map, ticket_cache,
                              existing_global, existing_ticket, global_unique_enabled,
                              dedup, expire_date, default_message):
    """Core logic to process a chunk of rows and prepare Invitation objects."""
    invites_to_create = []
    for row in chunk:
        if row.get("status") != "valid":
            continue

        ticket_name = (row.get("ticket_type") or "").strip().lower()
        ticket_type_id = ticket_map.get(ticket_name)
        if not ticket_type_id:
            continue

        email = row["guest_email"].lower()
        unique_code = uuid.uuid4()
        invite_url = f"{BASE_URL}{unique_code}"
        key_ticket = (email, ticket_name)
        key_global = email

        # Resolve deduplication scope
        scope = resolve_dedup_scope(email, ticket_name, ticket_cache, global_unique_enabled)
        is_dup = handle_deduplication(job, dedup, email, ticket_name, scope)
        if is_dup:
            continue

        # DB-level duplicate filtering
        ticket_type_obj = ticket_cache.get(ticket_name) if ticket_cache else None
        if not ticket_type_obj:
            continue

        enforce_unique = ticket_type_obj.get("enforce_unique_email", False)
        if global_unique_enabled and key_global in existing_global:
            continue
        elif enforce_unique and key_ticket in existing_ticket:
            continue

        invite = Invitation(
            user=job.user,
            guest_name=row["guest_name"].strip(),
            guest_email=email,
            company_name=row.get("company"),
            personal_message=row.get("personal_message") or default_message or "",
            ticket_type_id=ticket_type_id,
            expire_date=expire_date,
            source_type="bulk",
            link_code=unique_code,
            invitation_url=invite_url,
            usage_limit=1,
            status="active",
            is_sent=True,
        )
        invites_to_create.append(invite)

    return invites_to_create


def bulk_create_invitations(invites_to_create, created_total, pending_total):
    """Try to bulk create invitations, fallback to individual save on error."""
    try:
        Invitation.objects.bulk_create(invites_to_create, batch_size=BATCH_CREATE, ignore_conflicts=True)
        created_total += len(invites_to_create)
    except Exception as e:
        print(f"Bulk create failed — fallback to individual create: {e}")
        for invite in invites_to_create:
            try:
                invite.save()
                created_total += 1
            except Exception as inner_err:
                print(f"❌ Failed to create invite {invite.guest_email}: {inner_err}")
                invite.status = "pending"
                invite.is_sent = False
                invite.save(force_insert=True)
                pending_total += 1
    return created_total, pending_total


# ==============================
# 🔹 MAIN TASK
# ==============================

@shared_task(bind=True)
def send_bulk_invite(self, job_id, expire_date, default_message):
    """Main Celery Task: send invites for valid rows after user confirms."""
    try:
        job, dedup, redis_client = get_bulk_job_and_setup(job_id)
        rows, redis_key = fetch_rows_from_redis(redis_client, job_id)
        BASE_URL, ticket_map, stats, existing_global, existing_ticket, global_unique_enabled, ticket_cache = prepare_invitation_data(job)

        total = len(rows)
        created_total = 0
        pending_total = 0

        for start in range(0, total, BATCH_CREATE):
            end = start + BATCH_CREATE
            chunk = rows[start:end]

            invites_to_create = create_invitation_objects(
                chunk, job, BASE_URL, ticket_map, ticket_cache,
                existing_global, existing_ticket, global_unique_enabled,
                dedup, expire_date, default_message
            )

            created_total, pending_total = bulk_create_invitations(invites_to_create, created_total, pending_total)

            # Update stats
            stats.generated_invitations += len(invites_to_create)
            stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
            stats.save(update_fields=["generated_invitations", "remaining_invitations"])

            self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
            print(f"Created {created_total} active, {pending_total} pending so far...")

        # Job completion
        job.status = BulkUploadJob.STATUS_COMPLETED
        job.save(update_fields=["status", "updated_at"])
        delete_rows_key(job_id)

        print(f"Job {job_id} completed — {created_total} active, {pending_total} pending.")
        return {"created": created_total, "pending": pending_total}

    except Exception as e:
        print(f"Error in bulk job {job_id}: {e}")
        job = BulkUploadJob.objects.filter(id=job_id).first()
        if job:
            job.status = BulkUploadJob.STATUS_FAILED
            job.save(update_fields=["status"])
        raise



















# from celery import shared_task
# from invitations.models import BulkUploadJob, Invitation, InvitationStats
# from adminapp.models import TicketType, DuplicateRecord
# from invitations.utils.redis_utils import get_redis, delete_rows_key
# import orjson
# import uuid
# from decouple import config
# from ..utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context
# from invitations.deduplication.dedup_service import DeduplicationService
# from invitations.deduplication.utils import resolve_dedup_scope
# BATCH_CREATE = 5000  # Batch size for creating invitations

# @shared_task(bind=True)
# def send_bulk_invite(self, job_id, expire_date, default_message):
#     """
#     Sending invites for valid rows after user confirms.
#     The data is picked from Redis hash `bulk:job:{job_id}:rows`.
#     """
#     try:
#         #Concurrent dedupliate service 
#         dedup = DeduplicationService(namespace=f"bulk:{job_id}")

#         job = BulkUploadJob.objects.get(id=job_id)
#         job.status = BulkUploadJob.STATUS_SENDING
#         job.save(update_fields=["status"])

#         r = get_redis()
#         key = f"bulk:job:{job_id}:rows"
#         # Fetch all rows from Redis hash
#         rows_dict = r.hgetall(key)
#         rows = [orjson.loads(value) for value in rows_dict.values()]
#         total = len(rows)
#         created_total = 0
#         pending_total = 0

#         BASE_URL = config("FRONTEND_INVITE_URL", "http://178.18.253.63:3010/invite/")
#         ticket_map = {t.name.lower(): t.id for t in TicketType.objects.filter(is_active=True)}

#         stats, _ = InvitationStats.objects.get_or_create(user=job.user)

#         existing_raw = Invitation.objects.filter(user=job.user).values_list(
#             "guest_email", "ticket_type__name"
#         )
#         # global & ticket-level caches
#         existing_global = {email.lower() for email, _ in existing_raw if email}
#         existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}
        
#         global_unique_enabled, ticket_cache = load_ticket_email_validation_context()
#         # Process rows in batches
#         for start in range(0, total, BATCH_CREATE):
#             end = start + BATCH_CREATE
#             chunk = rows[start:end]
#             invites_to_create = []
#             # fallback_pending = []

#             for row in chunk:
#                 print("ROW", row)
#                 if row.get("status") != "valid":
#                     continue
                
#                 ticket_name = (row.get("ticket_type") or "").strip().lower()
#                 ticket_type_id = ticket_map.get(ticket_name)
#                 print("ticket_type_id", ticket_type_id)
#                 if not ticket_type_id:
#                     continue

#                 unique_code = uuid.uuid4()
#                 invite_url = f"{BASE_URL}{unique_code}"
#                 email = row["guest_email"].lower()
#                 key_ticket = (email, ticket_name)
#                 key_global = email
#                 ticket_type_obj = None

#                 #deduplicate - service for checking duplicate concurrent uploads.
#                 scope = resolve_dedup_scope(email, ticket_name, ticket_cache, global_unique_enabled)
#                 print("Duplicate Concurrent", scope)

#                 is_dup = False
#                 if scope != "none":
#                     is_dup = dedup.is_duplicate(
#                         user_id=job.user.id,
#                         email=email,
#                         ticket_type=ticket_name if scope == "ticket" else "",
#                     )

#                     if is_dup:
#                         DuplicateRecord.objects.create(
#                             user=job.user,
#                             job=job,
#                             guest_email=email,
#                             ticket_type=TicketType.objects.filter(name__iexact=ticket_name).first(),
#                             detection_source="dedup_service",
#                             scope=scope,
#                             reason=f"Duplicate {scope} for {email} ({ticket_name}) detected by dedup service",
#                         )
#                         continue

#                 if ticket_cache:
#                     ticket_type_obj = ticket_cache.get(ticket_name)
#                 if not ticket_type_obj:
#                     continue
#                 enforce_unique = ticket_type_obj.get("enforce_unique_email", False)

#                 if global_unique_enabled:
#                     print("GLOBAL  - DB level UNIQ ENFORCED", global_unique_enabled)
#                     if key_global in existing_global:
#                         continue
#                 elif enforce_unique:
#                     if key_ticket in existing_ticket:
#                         continue

#                 invite = Invitation(
#                     user=job.user,
#                     guest_name=row["guest_name"].strip(),
#                     guest_email=row["guest_email"].lower(),
#                     company_name=row.get("company"),
#                     personal_message=row.get("personal_message") or default_message or "",
#                     ticket_type_id=ticket_type_id,
#                     expire_date=expire_date,
#                     source_type="bulk",
#                     link_code=unique_code,
#                     invitation_url=invite_url,
#                     usage_limit=1,
#                     status="active",
#                     is_sent=True
#                 )

#                 invites_to_create.append(invite)

#             # Bulk create active invites
#             try:
#                 Invitation.objects.bulk_create(invites_to_create, batch_size=BATCH_CREATE, ignore_conflicts=True)
#                 created_total += len(invites_to_create)
#             except Exception as e:
#                 print(f"Bulk create failed for batch — falling back to individual creates: {e}")
#                 for invite in invites_to_create:
#                     try:
#                         invite.save()
#                         created_total += 1
#                     except Exception as inner_err:
#                         print(f"❌ Failed to create invite {invite.guest_email}: {inner_err}")
#                         invite.status = "pending"
#                         invite.is_sent = False
#                         invite.save(force_insert=True)
#                         pending_total += 1

#             # Update stats
#             stats.generated_invitations += len(invites_to_create)
#             stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
#             stats.save(update_fields=["generated_invitations", "remaining_invitations"])

#             self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
#             print(f"Created {created_total} active, {pending_total} pending so far...")

#         # Mark job as complete
#         job.status = BulkUploadJob.STATUS_COMPLETED
#         job.save(update_fields=["status", "updated_at"])
#         delete_rows_key(job_id)

#         print(f"Job {job_id} completed — {created_total} active, {pending_total} pending.")
#         return {"created": created_total, "pending": pending_total}

#     except Exception as e:
#         print(f"Error in bulk job {job_id}: {e}")
#         job = BulkUploadJob.objects.filter(id=job_id).first()
#         if job:
#             job.status = BulkUploadJob.STATUS_FAILED
#             job.save(update_fields=["status"])
#         raise