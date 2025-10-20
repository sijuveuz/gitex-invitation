from rest_framework.response import Response
from rest_framework import status
from invitations.models import BulkUploadJob


def handle_bulk_job_status(request, job_id):
    """
    Retrieves the current status and summary statistics
    for a specific bulk upload job. 
    """
    try:
        job = BulkUploadJob.objects.get(id=job_id, user=request.user)
    except BulkUploadJob.DoesNotExist:
        return Response(
            {"status": "error", "message": "Job not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    data = {
        "job_id": str(job.id),
        "status": job.status,
        "total_count": job.total_count,
        "valid_count": job.valid_count,
        "invalid_count": job.invalid_count,
        "created_count": getattr(job, "created_count", None),
        "updated_at": job.updated_at,
    }

    return Response(
        {
            "status": "success",
            "message": "Bulk job status fetched successfully.",
            "data": data,
        },
        status=status.HTTP_200_OK,
    )
