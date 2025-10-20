from decouple import config
from rest_framework.response import Response
from rest_framework import status
from invitations.serializers import BulkUploadCreateSerializer
from invitations.models import BulkUploadJob
from invitations.tasks.validate_bulk_csv_task import validate_csv_file_task


def handle_bulk_upload(request):
    """
    Handles the bulk invitation upload:
    - Validates uploaded CSV
    - Checks file size
    - Creates BulkUploadJob
    - Starts async validation
    """
    serializer = BulkUploadCreateSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    uploaded_file = serializer.validated_data["file"]

    # File size limit (default 5MB)
    MAX_FILE_SIZE = config("BULK_UPLOAD_MAX_SIZE", cast=int, default=5 * 1024 * 1024)
    if uploaded_file.size > MAX_FILE_SIZE:
        return Response(
            {"status": "error", "message": "File size exceeds 5MB limit"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Create job record
    job = BulkUploadJob.objects.create(
        user=request.user,
        uploaded_file=uploaded_file,
        file_name=getattr(uploaded_file, "name", ""),
        status=BulkUploadJob.STATUS_PENDING,
    )

    # Trigger async validation task
    validate_csv_file_task.delay(str(job.id))

    return Response(
        {
            "status": "success",
            "message": "File uploaded successfully. Validation started.",
            "data": {"job_id": str(job.id)},
        },
        status=status.HTTP_201_CREATED,
    )
