from rest_framework.response import Response
from rest_framework import status
from invitations.models import Invitation
from invitations.serializers import InvitationDetailSerializer


def handle_invitation_detail(request, link_code):
    """
    Handles fetching invitation details by link code,
    including validation for invalid or expired invitations.
    """
    try:
        invitation = Invitation.objects.select_related("ticket_type", "user").get(link_code=link_code)
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invalid or expired invitation."},
            status=status.HTTP_404_NOT_FOUND
        )

    # Check if expired
    if invitation.is_expired:
        invitation.mark_as_expired()
        return Response(
            {"status": "error", "message": "This invitation has expired."},
            status=status.HTTP_400_BAD_REQUEST
        )

    serializer = InvitationDetailSerializer(invitation)
    return Response(
        {
            "status": "success",
            "message": "Invitation details fetched successfully.",
            "data": serializer.data,
        },
        status=status.HTTP_200_OK
    )
