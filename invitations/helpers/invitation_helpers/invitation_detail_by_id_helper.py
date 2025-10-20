from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from invitations.models import Invitation
from invitations.serializers import InvitationDetailSerializerById


def handle_invitation_detail_by_id(request, pk):
    """
    Handles fetching an invitation by its ID.
    Includes auto-expiration logic for expired invitations.
    """
    try:
        invitation = Invitation.objects.get(id=pk, user=request.user)
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invitation not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    #Auto-expire if past expire_date and still active
    today = timezone.now().date()
    if invitation.status == "active" and invitation.expire_date < today:
        invitation.status = "expired"
        invitation.save(update_fields=["status"])

    serializer = InvitationDetailSerializerById(invitation)
    return Response(
        {"status": "success", "message": "Invitation details fetched.", "data": serializer.data},
        status=status.HTTP_200_OK,
    )
