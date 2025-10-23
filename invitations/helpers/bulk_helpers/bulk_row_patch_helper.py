from rest_framework.response import Response
from rest_framework import status
from threading import Lock

from invitations.models import BulkUploadJob, Invitation
from invitations.utils.redis_utils import range_rows, update_row, get_stats, incr_stats
from invitations.helpers.bulk_helpers.bulk_validator import load_ticket_types_cache, validate_row_csv_dict
from invitations.utils.bulk_email_uniqueness_validator import load_ticket_email_validation_context

def handle_bulk_row_patch(request, job_id, row_id):
    """Handles PATCH logic for updating a single row by id."""

    try:
        job = BulkUploadJob.objects.get(id=job_id, user=request.user)
    except BulkUploadJob.DoesNotExist:
        return Response(
            {"status": "error", "message": "Job not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Fetch the row by id
    rows = range_rows(job_id, id_list=[row_id])
    if not rows:
        return Response(
            {"status": "error", "message": f"Row with id {row_id} not found."},
            status=status.HTTP_404_NOT_FOUND,
        )
    current_row = rows[0]
    print(f"Updating row: id={row_id}, data={current_row}")
    edits = request.data

    # Apply incoming edits
    for k in ("guest_name", "guest_email", "ticket_type", "company", "personal_message"):
        if k in edits:
            current_row[k] = edits[k]

    # load_ticket_types_cache()

    # existing_invites = set(
    #     Invitation.objects.filter(user=request.user)
    #     .values_list("guest_email", "ticket_type__name", "user")
    # )

    # --- Prepare file-level duplicate maps (excluding current row) ---
    all_rows = range_rows(job_id)

    # file-level duplicate trackers (thread-safe)
    seen_global_dupes = {}
    seen_ticket_dupes = {}
    seen_lock = Lock()

    for r in all_rows:
        if r["id"] == row_id:
            continue
        email = (r.get("guest_email") or "").lower()
        ticket = (r.get("ticket_type") or "").lower()
        if email:
            seen_global_dupes[email] = r["row_number"]
        if email and ticket:
            seen_ticket_dupes[(email, ticket)] = r["row_number"]

    csv_like = {
        "Full Name": current_row.get("guest_name", ""),
        "Email": current_row.get("guest_email", ""),
        "Ticket Type": current_row.get("ticket_type", ""),
        "Company": current_row.get("company", ""),
        "Personal Message": current_row.get("personal_message", ""),
    }
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
    new_row_obj, _ = validate_row_csv_dict(
        csv_like,
        current_row["row_number"],
        existing_global=existing_global,
        existing_ticket=existing_ticket,
        ticket_cache=ticket_cache,
        global_unique_enabled=global_unique_enabled,
        seen_global_dupes=seen_global_dupes,
        seen_ticket_dupes=seen_ticket_dupes, 
        seen_lock=seen_lock

    )
    new_row_obj["id"] = row_id  # Preserve id

    # if new_row_obj.get("file_level_duplicate") or new_row_obj.get("duplicate"):
    #     return Response(
    #         {
    #             "status": "error",
    #             "message": "Duplicate entry detected (email + ticket type already exists).",
    #             "errors": new_row_obj.get("errors", {}),
    #         },
    #         status=status.HTTP_400_BAD_REQUEST,
    #     )

    # Update stats if status changes
    old_status = current_row["status"]
    new_status = new_row_obj["status"]
    if old_status != new_status:
        if new_status == "valid":
            incr_stats(job_id, "valid_count", 1)
            if old_status == "invalid":
                incr_stats(job_id, "invalid_count", -1)
        else:
            incr_stats(job_id, "invalid_count", 1)
            if old_status == "valid":
                incr_stats(job_id, "valid_count", -1)

    # Update Redis
    update_row(job_id, row_id, new_row_obj)

    # Get updated stats
    stats = get_stats(job_id)
    BulkUploadJob.objects.filter(id=job_id).update(
        total_count=stats["total_count"],
        valid_count=stats["valid_count"],
        invalid_count=stats["invalid_count"]
    )
    return Response(
        {
            "status": "success",
            "message": "Row updated successfully.",
            "data": new_row_obj,
            "stats": stats,
        },
        status=status.HTTP_200_OK,
    )

