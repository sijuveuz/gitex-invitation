from invitations.models import Invitation

def check_email_uniqueness(email, ticket_type, existing_global=None, existing_ticket=None):
    """
    Checks email uniqueness based on priority:
    1. Ticket-level uniqueness (if enabled)
    2. Global uniqueness (if ticket-level is off)
    Returns: (is_valid: bool, message: str | None, scope: str | None)
    """

    if not email or not ticket_type:
        return True, None, None

    ticket_unique = getattr(ticket_type, "enforce_unique_email", False)

    ticket_name = ticket_type.name.lower()

    # --- Priority 1: Ticket-level uniqueness ---
    if ticket_unique:
        if existing_ticket is not None:
            # Fast local check
            if (email, ticket_name) in existing_ticket:
                return False, f"Email already exists .", "ticket"
        else:
            if Invitation.objects.filter(guest_email=email, ticket_type=ticket_type).exists():
                return False, f"Email already exists.", "ticket"
        return True, None, "ticket"

    return True, None, None
