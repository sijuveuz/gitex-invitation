from celery import shared_task
from invitations.models import BulkUploadJob, Invitation, InvitationStats
from adminapp.models import TicketType
from invitations.utils.redis_utils import get_redis, delete_rows_key
import orjson
import uuid
import os
from django.core.files.storage import default_storage
from decouple import config
BATCH_CREATE = 5000  # Batch size for creating invitations

@shared_task(bind=True)
def send_bulk_invite(self, job_id, expire_date, default_message):
    """
    Sending invites for valid rows after user confirms.
    The data is picked from Redis hash `bulk:job:{job_id}:rows`.
    """
    try:
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

        # Process rows in batches
        for start in range(0, total, BATCH_CREATE):
            end = start + BATCH_CREATE
            chunk = rows[start:end]
            invites_to_create = []
            fallback_pending = []

            for row in chunk:
                if row.get("status") != "valid":
                    continue

                ticket_name = (row.get("ticket_type") or "").strip().lower()
                ticket_type_id = ticket_map.get(ticket_name)
                if not ticket_type_id:
                    continue

                unique_code = uuid.uuid4()
                invite_url = f"{BASE_URL}{unique_code}"

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