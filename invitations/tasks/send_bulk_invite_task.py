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
from django.core.exceptions import ValidationError

BATCH_CREATE = 5000  # Batch size for creating invitations


import logging
send_bulk_invite_logger = logging.getLogger("django")
# ==============================
# 🔹 HELPER FUNCTIONS
# ==============================

def get_bulk_job_and_setup(job_id):
    """Fetch BulkUploadJob, set status, and prepare dedup service + Redis."""
    job = BulkUploadJob.objects.get(id=job_id)
    job.status = BulkUploadJob.STATUS_SENDING
    job.save(update_fields=["status"])

    # dedup = DeduplicationService(namespace=f"bulk:{job_id}")
    #Common for all
    dedup = DeduplicationService(namespace=f"invite")
    send_bulk_invite_logger.info(f"✅ DeduplicationService initialised for Job id: {job_id}")

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
    stats, _ = InvitationStats.objects.get_or_create(id=1)

    existing_raw = Invitation.objects.values_list(
        "guest_email", "ticket_type__name"
    )
    existing_global = {email.lower() for email, _ in existing_raw if email}
    existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}

    ticket_cache = load_ticket_email_validation_context()
    return BASE_URL, ticket_map, stats, existing_global, existing_ticket, ticket_cache


def handle_deduplication(job, dedup, email, ticket_name, scope):
    """Check and log duplicates using the dedup service."""

    send_bulk_invite_logger.debug(f"SCOPE RECEIVED IN handle_deduplication : {scope}")
    is_dup = False
    if scope != "none":
        is_dup = dedup.is_duplicate(
            user_id=job.user.id,
            email=email,
            ticket_type=ticket_name if scope == "ticket" else "",
        )
        # if is_dup:
        #     DuplicateRecord.objects.create(
        #         user=job.user,
        #         job=job,
        #         guest_email=email,
        #         ticket_type=TicketType.objects.filter(name__iexact=ticket_name).first(),
        #         detection_source="dedup_service",
        #         scope=scope,
        #         reason=f"Duplicate {scope} for {email} ({ticket_name}) detected by dedup service",
        #     )
    return is_dup


def create_invitation_objects(chunk, job, BASE_URL, ticket_map, ticket_cache,
                              existing_global, existing_ticket,
                              dedup, expire_date, default_message):
    """Core logic to process a chunk of rows and prepare Invitation objects."""
    invites_to_create = []

    for row in chunk:   
        if row.get("status") != "valid":
            send_bulk_invite_logger.warning("❌ Row skipped because status != valid")
            continue

        ticket_name = (row.get("ticket_type") or "").strip().lower()
        ticket_type_id = ticket_map.get(ticket_name)
        if not ticket_type_id:
            send_bulk_invite_logger.error(f"❌ Invalid Ticket Type: '{ticket_name}' → Skipping Row")
            continue

        email = row["guest_email"].lower()
        unique_code = uuid.uuid4()
        invite_url = f"{BASE_URL}{unique_code}"
        key_ticket = (email, ticket_name)

        send_bulk_invite_logger.info(f"🔑 Processing Guest: Email={email}, Ticket={ticket_name}")

        # Determine dedup scope
        # scope = resolve_dedup_scope(ticket_name, ticket_cache)
        # send_bulk_invite_logger.info(f"🎯 Dedup Scope for Ticket '{ticket_name}' → {scope}")

        # # Perform dedup check
        # is_dup = handle_deduplication(job, dedup, email, ticket_name, scope)
        # send_bulk_invite_logger.info(f"📌 Dedup Result for ({email}, {ticket_name}, scope={scope}) → is_dup={is_dup}")

        # if is_dup:
        #     send_bulk_invite_logger.warning(f"🚫 Skipping — Deduplication detected duplicate → {email} / {ticket_name}")
        #     continue

        # DB-level duplicate fallback filter
        ticket_type_obj = ticket_cache.get(ticket_name) if ticket_cache else None
        if not ticket_type_obj:
            send_bulk_invite_logger.warning(f"❌ No Ticket Config Found for '{ticket_name}', skipping")
            continue

        enforce_unique = ticket_type_obj.get("enforce_unique_email", False)

        if enforce_unique:
            send_bulk_invite_logger.debug(f"🔒 Ticket '{ticket_name}' enforces unique email")

        if enforce_unique and key_ticket in existing_ticket:
            send_bulk_invite_logger.warning(
                f"🚫 DB Duplicate detected (already exists): {key_ticket} — Skipping"
            )
            continue

        # Create invitation object
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

        send_bulk_invite_logger.info(f"✅ Invitation queued for creation: {email} ({ticket_name})")

    send_bulk_invite_logger.info(f"🎉 Total invitations prepared in this chunk: {len(invites_to_create)}")
    return invites_to_create



def bulk_create_invitations(invites_to_create, created_total, pending_total):
    """Try to bulk create invitations, fallback to individual save on error."""
    try:
        Invitation.objects.bulk_create(
            invites_to_create,
            batch_size=BATCH_CREATE,
            ignore_conflicts=True
        )
        created_total += len(invites_to_create)
        send_bulk_invite_logger.info(
            f"✅ Bulk created {len(invites_to_create)} invitations successfully."
        )

    except Exception as e:
        send_bulk_invite_logger.error(
            f"⚠️ Bulk create failed — switching to individual save mode. Error: {e}"
        )

        for invite in invites_to_create:
            try:
                invite.save()
                created_total += 1
                send_bulk_invite_logger.debug(
                    f"✅ Individually created invitation for {invite.guest_email}"
                )

            except Exception as inner_err:
                send_bulk_invite_logger.error(
                    f"❌ Individual create failed for {invite.guest_email} — marking as pending. Error: {inner_err}"
                )

                # Mark as pending when direct save fails
                invite.status = "pending"
                invite.is_sent = False
                invite.save(force_insert=True)
                pending_total += 1

    return created_total, pending_total



# ==============================
# 🔹 MAIN TASK
# ==============================

from concurrent.futures import ThreadPoolExecutor, as_completed
from django.db import close_old_connections

MAX_WORKERS = 10  # You can tune based on your CPU and DB
CHUNK_SIZE = 3000

def save_invite(invite):
    """Worker function for parallel save."""
    close_old_connections()
    try:
        invite.save()
        return True
    except Exception as e:
        return False


@shared_task(bind=True)
def send_bulk_invite(self, job_id, expire_date, default_message):
    try:
        send_bulk_invite_logger.info(f"Bulk invite started for Job id: {job_id}")
        job, dedup, redis_client = get_bulk_job_and_setup(job_id)
        rows, redis_key = fetch_rows_from_redis(redis_client, job_id)
        BASE_URL, ticket_map, stats, existing_global, existing_ticket, ticket_cache = prepare_invitation_data(job)

        total = len(rows)
        created_total = 0
        pending_total = 0

        for start in range(0, total, BATCH_CREATE):
            end = start + BATCH_CREATE
            chunk = rows[start:end]

            invites_to_create = create_invitation_objects(
                chunk, job, BASE_URL, ticket_map, ticket_cache,
                existing_global, existing_ticket, dedup,
                expire_date, default_message
            )

            # ✅ Parallel save
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(save_invite, invite) for invite in invites_to_create]
                for f in as_completed(futures):
                    if f.result():
                        created_total += 1
                    else:
                        pending_total += 1

            send_bulk_invite_logger.info(
                f"✅ Batch {start//BATCH_CREATE + 1} done → Created: {created_total}, Pending: {pending_total}"
            )

            # Update stats
            stats.generated_invitations += len(invites_to_create)
            stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
            stats.save(update_fields=["generated_invitations", "remaining_invitations"])

            self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
            print(f"Created {created_total}, pending {pending_total}...")

        job.status = BulkUploadJob.STATUS_COMPLETED
        job.save(update_fields=["status", "updated_at"])
        delete_rows_key(job_id)

        return {"created": created_total, "pending": pending_total}

    except Exception as e:
        print(f"Error in bulk job {job_id}: {e}")
        job = BulkUploadJob.objects.filter(id=job_id).first()
        if job:
            job.status = BulkUploadJob.STATUS_FAILED
            job.error_note = str(e)
            job.save(update_fields=["status"])
        raise



# @shared_task(bind=True)
# def send_bulk_invite(self, job_id, expire_date, default_message):
#     """Main Celery Task: send invites for valid rows after user confirms."""
#     try:
#         send_bulk_invite_logger.info(f"Bulk invite started for Job id: {job_id}")
#         print("SENDING BUK INVITESSSSS")
#         job, dedup, redis_client = get_bulk_job_and_setup(job_id)
#         rows, redis_key = fetch_rows_from_redis(redis_client, job_id)
#         BASE_URL, ticket_map, stats, existing_global, existing_ticket, ticket_cache = prepare_invitation_data(job)

#         total = len(rows)
#         created_total = 0
#         pending_total = 0

#         for start in range(0, total, BATCH_CREATE):
#             end = start + BATCH_CREATE
#             chunk = rows[start:end]

#             # ✅ Log batch start
#             send_bulk_invite_logger.info(
#                 f"📦 Processing batch {start//BATCH_CREATE + 1}: rows {start} → {min(end, total)} (total: {total})"
#             )



#             invites_to_create = create_invitation_objects(
#                 chunk, job, BASE_URL, ticket_map, ticket_cache,
#                 existing_global, existing_ticket,
#                 dedup, expire_date, default_message
#             )

#             # created_total, pending_total = bulk_create_invitations(invites_to_create, created_total, pending_total)
#             for invite in invites_to_create:
#                 invite._bulk_job = job  # ✅ attach job reference for logging
#                 try:
#                     invite.save()
#                     created_total += 1
#                 except ValidationError:
#                     pending_total += 1


#             send_bulk_invite_logger.info(
#                 f"✅ Batch {start//BATCH_CREATE + 1} completed → Created: {created_total}, Pending: {pending_total}"
#             )
#             # Update stats
#             stats.generated_invitations += len(invites_to_create)
#             stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
#             stats.save(update_fields=["generated_invitations", "remaining_invitations"])

#             self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
#             print(f"Created {created_total} active, {pending_total} pending so far...")

#         # Job completion
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
#             job.error_note = str(e)
#             job.save(update_fields=["status"])
#         raise




