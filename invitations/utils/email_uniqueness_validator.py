from invitations.models import Invitation
from adminapp.models import TicketType, InvitationSettings

def check_email_uniqueness(user, email, ticket_type, existing_global=None, existing_ticket=None):
    """
    Checks email uniqueness based on priority:
    1. Ticket-level uniqueness (if enabled)
    2. Global uniqueness (if ticket-level is off)
    Returns: (is_valid: bool, message: str | None, scope: str | None)
    """

    if not email or not ticket_type:
        return True, None, None

    global_setting = InvitationSettings.objects.first()
    global_unique = getattr(global_setting, "enforce_global_unique", False)
    ticket_unique = getattr(ticket_type, "enforce_unique_email", False)

    ticket_name = ticket_type.name.lower()

    # --- Priority 1: Ticket-level uniqueness ---
    if ticket_unique:
        if existing_ticket is not None:
            # Fast local check
            if (email, ticket_name) in existing_ticket:
                return False, f"'{email}' already exists for this ticket type.", "ticket"
        else:
            if Invitation.objects.filter(user=user, guest_email=email, ticket_type=ticket_type).exists():
                return False, f"'{email}' already exists for this ticket type.", "ticket"
        return True, None, "ticket"

    # --- Priority 2: Global uniqueness ---
    if global_unique:
        if existing_global is not None:
            if email in existing_global:
                return False, f"'{email}' already exists globally.", "global"
        else:
            if Invitation.objects.filter(user=user, guest_email=email).exists():
                return False, f"'{email}' already exists globally.", "global"
        return True, None, "global"

    # --- Priority 3: No uniqueness enforced ---
    return True, None, None
