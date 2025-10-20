from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from invitations.models import Invitation
from invitations.serializers import InvitationDetailSerializer


def handle_invitation_edit(request, pk):
    user = request.user
    data = request.data

    try:
        invitation = Invitation.objects.get(id=pk, user=user)
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invitation not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    editable_fields = ["guest_name", "company_name", "ticket_type", "expire_date"]

    # ✅ Update only provided fields
    for field in editable_fields:
        if field in data and data[field] not in [None, ""]:
            setattr(invitation, field, data[field])

    # ✅ Prevent setting past expiry
    if "expire_date" in data:
        expire_date = data.get("expire_date")
        if expire_date and expire_date < timezone.now().date():
            return Response(
                {"status": "error", "message": "Expiry date cannot be in the past."},
                status=status.HTTP_400_BAD_REQUEST,
            )

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
