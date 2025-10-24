from rest_framework.response import Response
from rest_framework import status
from datetime import datetime
from django.utils import timezone

from invitations.models import Invitation
from adminapp.models import TicketType
from invitations.serializers import InvitationDetailSerializer


def handle_invitation_edit(request, pk):
    user = request.user
    data = request.data

    try:
        invitation = Invitation.objects.get(id=pk)
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invitation not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    editable_fields = [
        "guest_name",
        "company_name",
        "ticket_type",
        "expire_date",
        "usage_limit",
        "guest_email",
    ]

    for field in editable_fields:
        if field in data and data[field] not in [None, ""]:
            #Handle foreign key (ticket_type) properly
            if field == "ticket_type":
                ticket_value = data.get("ticket_type")

                try:
                    # Accept both ID and name
                    if str(ticket_value).isdigit():
                        ticket_obj = TicketType.objects.get(id=int(ticket_value))
                    else:
                        ticket_obj = TicketType.objects.get(name=ticket_value)
                    invitation.ticket_type = ticket_obj
                except TicketType.DoesNotExist:
                    return Response(
                        {"status": "error", "message": f"Invalid ticket type: {ticket_value}"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            else:
                setattr(invitation, field, data[field])

    # ✅ Handle date field validation
    if "expire_date" in data:
        expire_date = data.get("expire_date")
        if expire_date:
            if isinstance(expire_date, str):
                try:
                    expire_date = datetime.strptime(expire_date, "%Y-%m-%d").date()
                except ValueError:
                    return Response(
                        {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            if expire_date < timezone.now().date():
                return Response(
                    {"status": "error", "message": "Expiry date cannot be in the past."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            invitation.expire_date = expire_date

    invitation.updated_at = timezone.now()
    invitation.save(update_fields=[*editable_fields, "updated_at"])

    serializer = InvitationDetailSerializer(invitation)
    return Response(
        {
            "status": "success",
            "message": "Invitation updated successfully.",
            "data": serializer.data,
        },
        status=status.HTTP_200_OK,
    )