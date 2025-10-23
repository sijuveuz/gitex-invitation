from rest_framework.response import Response
from rest_framework import status
from threading import Lock

from invitations.models import BulkUploadJob, Invitation
from invitations.utils.redis_utils import push_row, get_stats, set_stats, range_rows
from invitations.helpers.bulk_helpers.bulk_validator import load_ticket_types_cache, validate_row_csv_dict
from invitations.utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context


def handle_bulk_add_row(request, job_id):
    """Handles adding a single row to an existing bulk upload job."""

    try:
        job = BulkUploadJob.objects.get(id=job_id, user=request.user)
    except BulkUploadJob.DoesNotExist:
        return Response(
            {"status": "error", "message": "Job not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    data = request.data
    required_fields = ["guest_name", "guest_email", "ticket_type"]
    missing = [f for f in required_fields if not data.get(f)]
    if missing:
        return Response(
            {"status": "error", "message": f"Missing fields: {', '.join(missing)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # --- Load global & ticket-level context once ---
    global_unique_enabled, ticket_cache = load_ticket_email_validation_context()

    # --- Prepare DB-level duplicate maps ---
    existing_global = set()
    existing_ticket = set()

    invitations_qs = Invitation.objects.filter(user=request.user).values_list(
        "guest_email", "ticket_type__name"
    )
    for email, ticket_name in invitations_qs:
        if email:
            existing_global.add(email.lower())
        if email and ticket_name:
            existing_ticket.add((email.lower(), ticket_name.lower()))

    # --- Prepare file-level duplicate maps ---
    existing_rows = range_rows(job_id)
    # file-level duplicate trackers (thread-safe)
    seen_global_dupes = {}
    seen_ticket_dupes = {}
    seen_lock = Lock()

    for r in existing_rows:
        email = (r.get("guest_email") or "").lower()
        ticket = (r.get("ticket_type") or "").lower()
        if email:
            seen_global_dupes[email] = r["row_number"]
        if email and ticket:
            seen_ticket_dupes[(email, ticket)] = r["row_number"]

    # --- Assign next row id & number ---
    next_id = max((r["id"] for r in existing_rows), default=0) + 1
    next_row_number = max((r["row_number"] for r in existing_rows), default=0) + 1

    # --- Build CSV-like row for validation ---
    csv_like = {
        "Full Name": data.get("guest_name", ""),
        "Email": data.get("guest_email", ""),
        "Ticket Type": data.get("ticket_type", ""),
        "Company": data.get("company", ""),
        "Personal Message": data.get("personal_message", ""),
    }

    # --- Run the unified validator ---
    new_row_obj, errors = validate_row_csv_dict(
        csv_like,
        next_row_number,
        existing_global=existing_global,
        existing_ticket=existing_ticket,
        seen_global_dupes=seen_global_dupes,
        seen_ticket_dupes=seen_ticket_dupes,
        ticket_cache=ticket_cache,
        global_unique_enabled=global_unique_enabled,
        seen_lock=seen_lock
    )
    new_row_obj["id"] = next_id

    if new_row_obj.get("file_level_duplicate") or new_row_obj.get("duplicate"):
        return Response(
            {
                "status": "error",
                "message": "Duplicate entry detected.",
                "errors": new_row_obj.get("errors", {}),
            },
            status=status.HTTP_400_BAD_REQUEST,
        ) 

    push_row(job_id, new_row_obj)

    stats = get_stats(job_id) or {}
    total_count = stats.get("total_count", 0) + 1
    valid_count = stats.get("valid_count", 0) + (1 if new_row_obj["status"] == "valid" else 0)
    invalid_count = stats.get("invalid_count", 0) + (1 if new_row_obj["status"] == "invalid" else 0)

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
            "message": "Row added successfully.",
            "data": new_row_obj,
            "stats": {
                "total_count": total_count,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
            },
        },
        status=status.HTTP_201_CREATED,
    )