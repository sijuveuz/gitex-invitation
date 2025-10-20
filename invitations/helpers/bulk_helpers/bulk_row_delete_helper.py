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
















# import orjson
# from rest_framework.response import Response
# from rest_framework import status
# from invitations.models import BulkUploadJob
# from invitations.utils.redis_utils import get_redis, get_stats, set_stats


# def handle_bulk_row_delete(request, job_id, row_number):
#     """
#     Handles deletion of a specific row from Redis for a bulk upload job
#     and updates the corresponding job statistics.
#     """
#     try:
#         # --- Validate job ownership ---
#         try:
#             job = BulkUploadJob.objects.get(id=job_id, user=request.user)
#         except BulkUploadJob.DoesNotExist:
#             return Response(
#                 {"status": "error", "message": "Job not found."},
#                 status=status.HTTP_404_NOT_FOUND,
#             )

#         r = get_redis()
#         redis_key = f"bulk:job:{job_id}:rows"

#         # --- Fetch all rows from Redis ---
#         all_rows_raw = r.lrange(redis_key, 0, -1)
#         if not all_rows_raw:
#             return Response(
#                 {"status": "error", "message": "No rows found for this job."},
#                 status=status.HTTP_404_NOT_FOUND,
#             )

#         deleted_row = None
#         remaining_raw = []

#         # --- Find and remove target row ---
#         for raw in all_rows_raw:
#             row = orjson.loads(raw)
#             if str(row.get("row_number")) == str(row_number):
#                 deleted_row = row
#                 continue
#             remaining_raw.append(raw)

#         if not deleted_row:
#             return Response(
#                 {"status": "error", "message": f"Row {row_number} not found."},
#                 status=status.HTTP_404_NOT_FOUND,
#             )

#         # --- Rewrite Redis list efficiently ---
#         pipe = r.pipeline(transaction=False)
#         r.delete(redis_key)
#         for chunk_start in range(0, len(remaining_raw), 1000):
#             pipe.rpush(redis_key, *remaining_raw[chunk_start:chunk_start + 1000])
#         pipe.execute()

#         # --- Update stats ---
#         stats = get_stats(job_id)
#         total_count = max(stats.get("total_count", len(remaining_raw)), 1) - 1
#         valid_count = stats.get("valid_count", 0)
#         invalid_count = stats.get("invalid_count", 0)

#         row_status = str(deleted_row.get("status", "")).lower()
#         if row_status == "valid" and valid_count > 0:
#             valid_count -= 1
#         elif row_status == "invalid" and invalid_count > 0:
#             invalid_count -= 1

#         set_stats(
#             job_id,
#             total_count=total_count,
#             valid_count=valid_count,
#             invalid_count=invalid_count,
#         )

#         return Response(
#             {
#                 "status": "success",
#                 "message": f"Row {row_number} deleted successfully.",
#                 "stats": {
#                     "total_count": total_count,
#                     "valid_count": valid_count,
#                     "invalid_count": invalid_count,
#                 },
#             },
#             status=status.HTTP_200_OK,
#         )

#     except Exception as e:
#         return Response(
#             {"status": "error", "message": "Unable to delete row.", "error": str(e)},
#             status=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         )
