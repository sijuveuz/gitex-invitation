import re
import json
from django.core.validators import validate_email
from django.core.exceptions import ValidationError as DjangoValidationError
from invitations.models import TicketType
from invitations.utils.redis_utils import get_redis
TICKET_CACHE_KEY = "ticket_types_cache"
name_re = re.compile(r"^[A-Za-z0-9\s\.'\-]{2,255}$")

def load_ticket_types_cache():
    """Load ticket types from cache or DB if not present."""
    r = get_redis()
    cached = r.get(TICKET_CACHE_KEY)
    if cached:
        return set(json.loads(cached))

    ticket_names = list(
        TicketType.objects.filter(is_active=True)
        .values_list("name", flat=True)
    )
    r.set(TICKET_CACHE_KEY, json.dumps(ticket_names), ex=3600)
    return set(ticket_names)

def clear_ticket_types_cache():
    r = get_redis()
    r.delete(TICKET_CACHE_KEY)

def normalize_ticket_type(value):
    """Normalize ticket type for comparison consistency."""
    if not value:
        return ""
    val = str(value).strip().lower()
    return val

def validate_row_csv_dict(
    row,
    row_number,
    default_message=None,
    existing_invites=None,
    seen_file_duplicates=None,
    seen_lock=None
):
    errors = {}
    ticket_types = load_ticket_types_cache()

    # --- Full Name ---
    name = (row.get("Full Name") or row.get("full_name") or "").strip()
    if not name or len(name) < 2 or not name_re.match(name):
        errors["guest_name"] = "Full Name is required (min 2 chars)."

    # --- Email ---
    email = (row.get("Email") or row.get("email") or "").strip().lower()
    try:
        validate_email(email)
    except DjangoValidationError:
        errors["guest_email"] = "Invalid email format."

    # --- Ticket Type ---
    ticket_raw = (row.get("Ticket Type") or row.get("ticket_type") or "").strip()
    ticket_norm = normalize_ticket_type(ticket_raw)
    ticket_norm_l = ticket_norm.lower()
    valid_ticket_names = [t.lower() for t in ticket_types]
    if ticket_norm_l not in valid_ticket_names:
        errors["ticket_type"] = "Select valid ticket."

    # --- Company & Message ---
    company = (row.get("Company") or row.get("company") or "").strip()
    pm = (row.get("Personal Message") or row.get("personal_message") or "").strip()
    if not pm and default_message:
        pm = default_message[:500]

    # --- DB-Level Duplicate Check ---
    duplicate_db = False
    if existing_invites and email and ticket_norm:
        key_db = (email, ticket_norm_l)
        if key_db in existing_invites:
            duplicate_db = True
            errors["duplicate"] = (
                "Duplicate invitation detected for this email and ticket type."
            )

    # --- File-Level Duplicate Check ---
    file_level_duplicate = False
    if seen_file_duplicates is not None and email and ticket_norm:
        key_file = (email, ticket_norm_l)
        if seen_lock:
            with seen_lock:
                if key_file in seen_file_duplicates:
                    file_level_duplicate = True
                    errors["file_level_duplicate"] = (
                        f"Duplicate in file (also in row {seen_file_duplicates[key_file]})"
                    )
                else:
                    seen_file_duplicates[key_file] = row_number
        else:
            if key_file in seen_file_duplicates:
                file_level_duplicate = True
                errors["file_level_duplicate"] = (
                    f"Duplicate in file (also in row {seen_file_duplicates[key_file]})"
                )
            else:
                seen_file_duplicates[key_file] = row_number

    # --- Determine Row Status ---
    status = "valid" if not errors else "invalid"

    # --- Final Row Object ---
    row_obj = {
        "id": row.get("id", row_number),  # Use provided id or row_number (for bulk)
        "row_number": row_number,
        "guest_name": name,
        "guest_email": email,
        "ticket_type": ticket_norm or ticket_raw,
        "company": company,
        "personal_message": pm,
        "status": status,
        "error_found": bool(errors),
        "errors": errors,
        "duplicate": duplicate_db,
        "file_level_duplicate": file_level_duplicate,
    }

    return row_obj, errors
