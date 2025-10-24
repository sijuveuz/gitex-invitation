from django.db import transaction
from rest_framework.response import Response
from rest_framework import status

from invitations.models import BulkUploadJob, InvitationStats
from invitations.tasks.send_bulk_invite_task import send_bulk_invite


def handle_bulk_confirm_request(request, job_id):
    """
    Handles confirming a bulk upload job — validates quota, updates status,
    and triggers background processing for invitations.
    """
    try:
        with transaction.atomic():
            # --- Validate job ownership ---
            try:
                job = BulkUploadJob.objects.select_for_update().get(id=job_id, user=request.user)
            except BulkUploadJob.DoesNotExist:
                return Response(
                    {"status": "error", "message": "Job not found."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            expire_date = request.data.get("expire_date", None)
            default_message = request.data.get("default_personal_message")

            # --- Check job status ---
            if job.status != BulkUploadJob.STATUS_PREVIEW_READY:
                return Response(
                    {"status": "error", "message": "File is not ready for processing."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # --- Fetch or create user stats ---
            stats, _ = InvitationStats.objects.select_for_update().get_or_create(id=1)

            total_to_generate = job.valid_count or 0
            if total_to_generate == 0:
                return Response(
                    {"status": "error", "message": "No valid invitations found to process."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # --- Check quota ---
            if stats.remaining_invitations < total_to_generate:
                shortfall = total_to_generate - stats.remaining_invitations
                return Response(
                    {
                        "status": "error",
                        "message": (
                            f"Quota exceeded. You have only {stats.remaining_invitations} remaining, "
                            f"but trying to send {total_to_generate}. "
                            f"Please remove some rows or upgrade your plan (+{shortfall} more needed)."
                        ),
                        "code": "INSUFFICIENT_QUOTA",
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # --- Update status and trigger task ---
            job.status = BulkUploadJob.STATUS_CONFIRMED
            job.save(update_fields=["status"])

            send_bulk_invite.delay(str(job.id), expire_date, default_message)

            return Response(
                {"status": "success", "message": "Processing started."},
                status=status.HTTP_202_ACCEPTED,
            )

    except Exception as e:
        return Response(
            {"status": "error", "message": "Unable to confirm job.", "error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
