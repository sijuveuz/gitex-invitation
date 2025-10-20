from rest_framework.response import Response
from rest_framework import status
from invitations.models import BulkUploadJob
from invitations.utils.redis_utils import range_rows, get_stats, get_status
from rest_framework.response import Response
from rest_framework import status
from invitations.models import BulkUploadJob
from invitations.utils.redis_utils import get_redis, range_rows, get_stats, get_status
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

def get_job_or_404(job_id, user):
    """Fetch job or return None (for quick inline 404 check)."""
    try:
        return BulkUploadJob.objects.get(id=job_id, user=user)
    except BulkUploadJob.DoesNotExist:
        return None
 

def apply_row_filters(rows, request):
    """Apply basic filters from query params."""
    status_filter = request.query_params.get("status")
    ticket_type_filter = request.query_params.get("ticket_type")
    search_filter = request.query_params.get("search")

    def match(row):
        if status_filter and row.get("status") != status_filter.lower():
            return False
        if ticket_type_filter and ticket_type_filter.lower() not in row.get("ticket_type", "").lower():
            return False
        if search_filter:
            search = search_filter.lower()
            if not (
                search in row.get("guest_name", "").lower()
                or search in row.get("guest_email", "").lower()
                or search in row.get("company_name", "").lower()
            ):
                return False
        return True

    return [r for r in rows if match(r)]


def handle_bulk_rows_request(request, job_id):
    """Handles GET request to fetch rows for a bulk upload job with pagination and filtering."""
    try:
        job = BulkUploadJob.objects.get(id=job_id, user=request.user)
    except BulkUploadJob.DoesNotExist:
        return Response(
            {"status": "error", "message": "Job not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Get query parameters
    page = request.query_params.get("page", 1)
    per_page = request.query_params.get("per_page", 10)
    search = request.query_params.get("search", "").strip().lower()
    status_filter  = request.query_params.get("status", "").lower()
    ticket_type = request.query_params.get("ticket_type", "").strip().lower()
    print("STATUS:",status_filter)
    try:
        page = int(page)
        per_page = int(per_page)
    except ValueError:
        return Response(
            {"status": "error", "message": "Invalid page or per_page parameter."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Fetch all rows
    all_rows = range_rows(job_id)
    
    # Apply filters
    filtered_rows = all_rows
    if search:
        filtered_rows = [
            row for row in filtered_rows
            if (search in (row.get("guest_name", "") or "").lower() or
                search in (row.get("guest_email", "") or "").lower())
        ]
    if status_filter in ("valid", "invalid"):
        filtered_rows = [
            row for row in filtered_rows
            if row.get("status", "").lower() == status_filter
        ]
    if ticket_type:
        filtered_rows = [
            row for row in filtered_rows
            if (row.get("ticket_type", "") or "").lower() == ticket_type
        ]

    # Paginate filtered rows
    paginator = Paginator(filtered_rows, per_page)
    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    # Update stats for filtered results
    stats = get_stats(job_id)
    filtered_stats = {
        "total_count": len(filtered_rows),
        "valid_count": len([r for r in filtered_rows if r.get("status") == "valid"]),
        "invalid_count": len([r for r in filtered_rows if r.get("status") == "invalid"]),
    }

    return Response(
        {
            "status": "success",
            "message": "Rows fetched successfully.",
            "data": page_obj.object_list,
            "stats": filtered_stats,
            "job_status": get_status(job_id),
            "pagination": {
                "current_page": page_obj.number,
                "per_page": per_page,
                "total_pages": paginator.num_pages,
                "total_rows": paginator.count,
            },
        },
        status=status.HTTP_200_OK,
    )