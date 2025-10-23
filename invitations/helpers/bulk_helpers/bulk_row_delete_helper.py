from rest_framework.response import Response
from rest_framework import status
from invitations.models import BulkUploadJob
from invitations.utils.redis_utils import get_redis, range_rows, delete_row, get_stats, set_stats
import orjson
def handle_bulk_row_delete(request, job_id, row_id):
    """Handles deletion of a specific row from Redis by id."""

    try:
        try:
            job = BulkUploadJob.objects.get(id=job_id, user=request.user)
        except BulkUploadJob.DoesNotExist:
            return Response(
                {"status": "error", "message": "Job not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        r = get_redis()
        redis_key = f"bulk:job:{job_id}:rows"

        # Check if row exists
        if not r.hexists(redis_key, str(row_id)):
            return Response(
                {"status": "error", "message": f"Row with id {row_id} not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Fetch row for status
        row = orjson.loads(r.hget(redis_key, str(row_id)))
        row_status = row.get("status", "").lower()

        # Delete row
        delete_row(job_id, row_id)

        # Update stats
        stats = get_stats(job_id)
        total_count = max(stats.get("total_count", 0), 1) - 1
        valid_count = stats.get("valid_count", 0)
        invalid_count = stats.get("invalid_count", 0)

        if row_status == "valid" and valid_count > 0:
            valid_count -= 1
        elif row_status == "invalid" and invalid_count > 0:
            invalid_count -= 1

        set_stats(
            job_id,
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
        )

        job.total_count = total_count
        job.valid_count = valid_count
        job.invalid_count = invalid_count
        job.save(update_fields=["total_count", "valid_count", "invalid_count", "updated_at"])

        return Response(
            {
                "status": "success",
                "message": f"Row with id {row_id} deleted successfully.",
                "stats": {
                    "total_count": total_count,
                    "valid_count": valid_count,
                    "invalid_count": invalid_count,
                },
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        return Response(
            {"status": "error", "message": "Unable to delete row.", "error": str(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )


