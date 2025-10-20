from rest_framework.response import Response
from rest_framework import status
from invitations.models import BulkUploadJob
from invitations.utils.redis_utils import delete_rows_key, set_stats, set_status


def handle_bulk_clear_preview(request, job_id):
    """
    Handles clearing all preview rows and resetting stats for a bulk upload job.
    """
    try:
        job = BulkUploadJob.objects.get(id=job_id, user=request.user)

        # Delete all rows from Redis 
        delete_rows_key(job_id)

        # Reset stats and status in Redis
        set_stats(job_id, total_count=0, valid_count=0, invalid_count=0)
        set_status(job_id, BulkUploadJob.STATUS_PENDING)

        # Reflect changes in DB
        job.total_count = 0
        job.valid_count = 0
        job.invalid_count = 0
        job.status = BulkUploadJob.STATUS_CLEARED
        job.save(update_fields=["total_count", "valid_count", "invalid_count", "status", "updated_at"])

        return Response(
            {
                "status": "success",
                "message": f"Preview data cleared for job {job_id}.",
                "data": {
                    "stats": {"total_count": 0, "valid_count": 0, "invalid_count": 0},
                    "job_status": BulkUploadJob.STATUS_CLEARED
                },
            },
            status=status.HTTP_200_OK,
        )

    except BulkUploadJob.DoesNotExist:
        return Response(
            {"status": "error", "message": "Job not found."},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception as e:
        return Response(
            {"status": "error", "message": "Unable to clear.", "error": str(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )
