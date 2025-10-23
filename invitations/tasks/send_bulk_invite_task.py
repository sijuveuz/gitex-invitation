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


# # -----------------------------
# # HELPER FUNCTIONS
# # -----------------------------
# def load_bulk_context(job_id):
#     """Load Redis data, job object, ticket map, stats, and dedup service."""
#     dedup = DeduplicationService(namespace=f"bulk:{job_id}")
#     job = BulkUploadJob.objects.get(id=job_id)
#     r = get_redis()
#     key = f"bulk:job:{job_id}:rows"

#     rows_dict = r.hgetall(key)
#     rows = [orjson.loads(value) for value in rows_dict.values()]

#     ticket_map = {t.name.lower(): t.id for t in TicketType.objects.filter(is_active=True)}
#     stats, _ = InvitationStats.objects.get_or_create(user=job.user)

#     existing_raw = Invitation.objects.filter(user=job.user).values_list(
#         "guest_email", "ticket_type__name"
#     )
#     existing_global = {email.lower() for email, _ in existing_raw if email}
#     existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}

#     global_unique_enabled, ticket_cache = load_ticket_email_validation_context()

#     return {
#         "dedup": dedup,
#         "job": job,
#         "rows": rows,
#         "ticket_map": ticket_map,
#         "stats": stats,
#         "existing_global": existing_global,
#         "existing_ticket": existing_ticket,
#         "global_unique_enabled": global_unique_enabled,
#         "ticket_cache": ticket_cache,
#     }


# def prepare_invitation(row, job, default_message, expire_date, BASE_URL, ticket_map, ticket_cache,
#                        global_unique_enabled, existing_global, existing_ticket, dedup):
#     """Validate and build a single Invitation object."""
#     if row.get("status") != "valid":
#         return None

#     ticket_name = (row.get("ticket_type") or "").strip().lower()
#     ticket_type_id = ticket_map.get(ticket_name)
#     if not ticket_type_id:
#         return None

#     email = row["guest_email"].lower()
#     unique_code = uuid.uuid4()
#     invite_url = f"{BASE_URL}{unique_code}"

#     # Deduplication
#     scope = resolve_dedup_scope(email, ticket_name, ticket_cache, global_unique_enabled)
#     if scope == "none":
#         return None

#     is_dup = dedup.is_duplicate(
#         user_id=job.user.id,
#         email=email,
#         ticket_type=ticket_name if scope == "ticket" else "",
#         scope=scope,
#     )
#     if is_dup:
#         DuplicateRecord.objects.create(
#             user=job.user,
#             job=job,
#             guest_email=email,
#             ticket_type=TicketType.objects.filter(name__iexact=ticket_name).first(),
#             detection_source="dedup_service",
#             scope=scope,
#             reason=f"Duplicate {scope} for {email} ({ticket_name}) detected by dedup service",
#         )
#         continue

#     ticket_type_obj = ticket_cache.get(ticket_name) if ticket_cache else None
#     if not ticket_type_obj:
#         return None

#     enforce_unique = ticket_type_obj.get("enforce_unique_email", False)
#     key_ticket = (email, ticket_name)
#     key_global = email

#     if global_unique_enabled:
#         if key_global in existing_global:
#             return None
#     elif enforce_unique:
#         if key_ticket in existing_ticket:
#             return None

#     # Construct Invitation object
#     return Invitation(
#         user=job.user,
#         guest_name=row["guest_name"].strip(),
#         guest_email=row["guest_email"].lower(),
#         company_name=row.get("company"),
#         personal_message=row.get("personal_message") or default_message or "",
#         ticket_type_id=ticket_type_id,
#         expire_date=expire_date,
#         source_type="bulk",
#         link_code=unique_code,
#         invitation_url=invite_url,
#         usage_limit=1,
#         status="active",
#         is_sent=True,
#     )


# def create_invites_bulk(invites_to_create):
#     """Try to bulk create; fallback to individual insert on failure."""
#     created_total = 0
#     pending_total = 0
#     try:
#         Invitation.objects.bulk_create(invites_to_create, batch_size=BATCH_CREATE, ignore_conflicts=True)
#         created_total = len(invites_to_create)
#     except Exception as e:
#         print(f"Bulk create failed — fallback to individual save: {e}")
#         for invite in invites_to_create:
#             try:
#                 invite.save()
#                 created_total += 1
#             except Exception as inner_err:
#                 print(f"❌ Failed to create invite {invite.guest_email}: {inner_err}")
#                 invite.status = "pending"
#                 invite.is_sent = False
#                 invite.save(force_insert=True)
#                 pending_total += 1
#     return created_total, pending_total


# def update_invitation_stats(stats, created_count):
#     """Update invitation stats after each batch."""
#     stats.generated_invitations += created_count
#     stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
#     stats.save(update_fields=["generated_invitations", "remaining_invitations"])


# # -----------------------------
# # MAIN TASK
# # -----------------------------
# @shared_task(bind=True)
# def send_bulk_invite(self, job_id, expire_date, default_message):
#     """
#     Sending invites for valid rows after user confirms.
#     The data is picked from Redis hash `bulk:job:{job_id}:rows`.
#     """
#     try:
#         ctx = load_bulk_context(job_id)
#         job = ctx["job"]
#         dedup = ctx["dedup"]
#         rows = ctx["rows"]
#         ticket_map = ctx["ticket_map"]
#         stats = ctx["stats"]
#         existing_global = ctx["existing_global"]
#         existing_ticket = ctx["existing_ticket"]
#         global_unique_enabled = ctx["global_unique_enabled"]
#         ticket_cache = ctx["ticket_cache"]

#         BASE_URL = config("FRONTEND_INVITE_URL", "http://178.18.253.63:3010/invite/")

#         job.status = BulkUploadJob.STATUS_SENDING
#         job.save(update_fields=["status"])

#         total = len(rows)
#         created_total = 0
#         pending_total = 0

#         for start in range(0, total, BATCH_CREATE):
#             end = start + BATCH_CREATE
#             chunk = rows[start:end]
#             invites_to_create = []

#             for row in chunk:
#                 invite = prepare_invitation(
#                     row=row,
#                     job=job,
#                     default_message=default_message,
#                     expire_date=expire_date,
#                     BASE_URL=BASE_URL,
#                     ticket_map=ticket_map,
#                     ticket_cache=ticket_cache,
#                     global_unique_enabled=global_unique_enabled,
#                     existing_global=existing_global,
#                     existing_ticket=existing_ticket,
#                     dedup=dedup,
#                 )
#                 if invite:
#                     invites_to_create.append(invite)

#             batch_created, batch_pending = create_invites_bulk(invites_to_create)
#             created_total += batch_created
#             pending_total += batch_pending

#             update_invitation_stats(stats, batch_created)
#             self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
#             print(f"Created {created_total} active, {pending_total} pending so far...")

#         job.status = BulkUploadJob.STATUS_COMPLETED
#         job.save(update_fields=["status", "updated_at"])
#         delete_rows_key(job_id)

#         print(f"✅ Job {job_id} completed — {created_total} active, {pending_total} pending.")
#         return {"created": created_total, "pending": pending_total}

#     except Exception as e:
#         print(f"❌ Error in bulk job {job_id}: {e}")
#         job = BulkUploadJob.objects.filter(id=job_id).first()
#         if job:
#             job.status = BulkUploadJob.STATUS_FAILED
#             job.save(update_fields=["status"])
#         raise





















from celery import shared_task
from invitations.models import BulkUploadJob, Invitation, InvitationStats
from adminapp.models import TicketType, DuplicateRecord
from invitations.utils.redis_utils import get_redis, delete_rows_key
import orjson
import uuid
from decouple import config
from ..utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context
from invitations.deduplication.dedup_service import DeduplicationService
from invitations.deduplication.utils import resolve_dedup_scope
BATCH_CREATE = 5000  # Batch size for creating invitations

@shared_task(bind=True)
def send_bulk_invite(self, job_id, expire_date, default_message):
    """
    Sending invites for valid rows after user confirms.
    The data is picked from Redis hash `bulk:job:{job_id}:rows`.
    """
    try:
        #Concurrent dedupliate service 
        dedup = DeduplicationService(namespace=f"bulk:{job_id}")

        job = BulkUploadJob.objects.get(id=job_id)
        job.status = BulkUploadJob.STATUS_SENDING
        job.save(update_fields=["status"])

        r = get_redis()
        key = f"bulk:job:{job_id}:rows"
        # Fetch all rows from Redis hash
        rows_dict = r.hgetall(key)
        rows = [orjson.loads(value) for value in rows_dict.values()]
        total = len(rows)
        created_total = 0
        pending_total = 0

        BASE_URL = config("FRONTEND_INVITE_URL", "http://178.18.253.63:3010/invite/")
        ticket_map = {t.name.lower(): t.id for t in TicketType.objects.filter(is_active=True)}

        stats, _ = InvitationStats.objects.get_or_create(user=job.user)

        existing_raw = Invitation.objects.filter(user=job.user).values_list(
            "guest_email", "ticket_type__name"
        )
        # global & ticket-level caches
        existing_global = {email.lower() for email, _ in existing_raw if email}
        existing_ticket = {(email.lower(), (t or "").lower()) for email, t in existing_raw if email and t}
        
        global_unique_enabled, ticket_cache = load_ticket_email_validation_context()
        # Process rows in batches
        for start in range(0, total, BATCH_CREATE):
            end = start + BATCH_CREATE
            chunk = rows[start:end]
            invites_to_create = []
            # fallback_pending = []

            for row in chunk:
                if row.get("status") != "valid":
                    continue
                
                ticket_name = (row.get("ticket_type") or "").strip().lower()
                ticket_type_id = ticket_map.get(ticket_name)
                if not ticket_type_id:
                    continue

                unique_code = uuid.uuid4()
                invite_url = f"{BASE_URL}{unique_code}"
                email = row["guest_email"].lower()
                key_ticket = (email, ticket_name)
                key_global = email
                ticket_type_obj = None

                #deduplicate - service for checking duplicate concurrent uploads.
                scope = resolve_dedup_scope(email, ticket_name, ticket_cache, global_unique_enabled)

                if scope == "none":
                    continue  # skip dedup check

                is_dup = dedup.is_duplicate(
                    user_id=job.user.id,
                    email=email,
                    ticket_type=ticket_name if scope == "ticket" else "",
                    scope=scope,
                )

                if is_dup:
                    # errors.append(f"Duplicate ({scope}) → {email} / {ticket_name}")
                    DuplicateRecord.objects.create(
                        user=job.user,
                        job=job,
                        guest_email=email,
                        ticket_type=TicketType.objects.filter(name__iexact=ticket_name).first(),
                        detection_source="dedup_service",
                        scope=scope,
                        reason=f"Duplicate {scope} for {email} ({ticket_name}) detected by dedup service",
                    )
                    continue

                if ticket_cache:
                    ticket_type_obj = ticket_cache.get(ticket_name)
                if not ticket_type_obj:
                    continue
                enforce_unique = ticket_type_obj.get("enforce_unique_email", False)

                if global_unique_enabled:
                    print("GLOBAL  - DB level UNIQ ENFORCED", global_unique_enabled)
                    if key_global in existing_global:
                        continue
                elif enforce_unique:
                    if key_ticket in existing_ticket:
                        continue

                invite = Invitation(
                    user=job.user,
                    guest_name=row["guest_name"].strip(),
                    guest_email=row["guest_email"].lower(),
                    company_name=row.get("company"),
                    personal_message=row.get("personal_message") or default_message or "",
                    ticket_type_id=ticket_type_id,
                    expire_date=expire_date,
                    source_type="bulk",
                    link_code=unique_code,
                    invitation_url=invite_url,
                    usage_limit=1,
                    status="active",
                    is_sent=True
                )

                invites_to_create.append(invite)

            # Bulk create active invites
            try:
                Invitation.objects.bulk_create(invites_to_create, batch_size=BATCH_CREATE, ignore_conflicts=True)
                created_total += len(invites_to_create)
            except Exception as e:
                print(f"Bulk create failed for batch — falling back to individual creates: {e}")
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

            # Update stats
            stats.generated_invitations += len(invites_to_create)
            stats.remaining_invitations = max(stats.allocated_invitations - stats.generated_invitations, 0)
            stats.save(update_fields=["generated_invitations", "remaining_invitations"])

            self.update_state(state="PROGRESS", meta={"created": created_total, "pending": pending_total})
            print(f"Created {created_total} active, {pending_total} pending so far...")

        # Mark job as complete
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