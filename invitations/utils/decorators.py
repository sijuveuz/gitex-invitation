from functools import wraps
from rest_framework.exceptions import ValidationError
from invitations.utils.email_uniqueness_validator import check_email_uniqueness
from invitations.serializers import PersonalizedInvitationSerializer

from adminapp.models import TicketType


def validate_email_uniqueness(func):
    """
    Enforces email uniqueness before creating an invitation.
    Delegates logic to `check_email_uniqueness` for consistency.
    """

    @wraps(func)
    def wrapper(user, data, *args, **kwargs):
        email = data.get("guest_email", "").lower().strip()
        ticket_field = data.get("ticket_type")

        if not email or not ticket_field:
            return func(user, data, *args, **kwargs)

        # 🧩 Handle both string and object cases
        if isinstance(ticket_field, TicketType):
            ticket_type = ticket_field
        else:
            try:
                ticket_type = TicketType.objects.get(name=str(ticket_field).strip())
            except TicketType.DoesNotExist:
                raise ValidationError({"detail": f"Invalid ticket type '{ticket_field}'."})

        # ✅ Reuse the same uniqueness logic
        is_valid, msg, scope = check_email_uniqueness(user, email, ticket_type)
        if not is_valid:
            raise ValidationError({"guest_email": msg})
 
        data["uniqueness_scope"] = scope
        return func(user, data, *args, **kwargs)

    return wrapper

