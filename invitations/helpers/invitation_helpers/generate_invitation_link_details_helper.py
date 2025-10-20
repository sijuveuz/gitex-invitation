from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from invitations.models import Invitation
from invitations.serializers import InvitationListSerializer


def handle_generate_invitation_link_details(request, link_code):
    """
    Handles retrieval of a link-based invitation by its UUID.
    Validates existence and expiry before returning details.
    """
    try:
        invitation = Invitation.objects.get(source_type="link", link_code=link_code)
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invalid or expired invitation link."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Check if expired
    if invitation.expire_date < timezone.now().date() or invitation.status == "expired":
        invitation.mark_as_expired()
        return Response(
            {"status": "error", "message": "This invitation link has expired."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Serialize and return
    serializer = InvitationListSerializer(invitation)
    return Response(
        {"status": "success", "message": "Invitation link details fetched.", "data": serializer.data},
        status=status.HTTP_200_OK,
    )
