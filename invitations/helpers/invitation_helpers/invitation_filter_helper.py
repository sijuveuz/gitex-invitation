from django.utils import timezone
from django.db.models import Q



from django.db.models import Q
from django.utils import timezone

def apply_invitation_filters(queryset, params):
    keyword = params.get("search")
    status = params.get("status")
    invite_type = params.get("type")
    expiry = params.get("expiry_date")
    ordering = params.get("ordering")
    ticket_type = params.get("ticket_type")

    # 🔍 Keyword search: guest name OR email
    if keyword:
        queryset = queryset.filter(
            Q(guest_name__icontains=keyword) |
            Q(guest_email__icontains=keyword)
        )

    # 🟩 Status filter
    if status and status.lower() != "all":
        queryset = queryset.filter(status__iexact=status)

    # 🟨 Invitation type filter
    if invite_type and invite_type.lower() != "all":
        if invite_type.lower() == "personalized":
            queryset = queryset.filter(source_type="personal")
        elif invite_type.lower() == "invitation link":
            queryset = queryset.filter(source_type="link")
        elif invite_type.lower() == "bulk upload":
            queryset = queryset.filter(source_type="bulk")

    # 🗓 Expiry date filter
    if expiry:
        queryset = queryset.filter(expire_date=expiry)

    # 🎟 Ticket type filter (NEW)
    if ticket_type and ticket_type.lower() != "all":
        # Try matching by name (case-insensitive) or ID (numeric)
        if ticket_type.isdigit():
            queryset = queryset.filter(ticket_type_id=int(ticket_type))
        else:
            queryset = queryset.filter(ticket_type__name__iexact=ticket_type)

    # ⏰ Auto-expire logic — mark active but past-date invites as expired
    queryset.filter(
        expire_date__lt=timezone.now().date(), status="active"
    ).update(status="expired")

    # 🔢 Sorting (example: ?ordering=-created_at or ?ordering=guest_name)
    if ordering:
        queryset = queryset.order_by(ordering)

    return queryset

