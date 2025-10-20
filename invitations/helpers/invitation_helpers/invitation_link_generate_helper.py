from rest_framework.response import Response
from rest_framework import status
from invitations.models import InvitationStats
from invitations.serializers import (
    InvitationLinkGenerateSerializer,
    InvitationStatsSerializer,
)


def handle_invitation_link_generate(request):
    """
    Handles creation of an invitation link and returns the user's updated invitation stats.
    """
    serializer = InvitationLinkGenerateSerializer(data=request.data, context={"request": request})
    if not serializer.is_valid():
        return Response(
            {"status": "error", "message": "Invalid data", "errors": serializer.errors},
            status=status.HTTP_400_BAD_REQUEST,
        )

    serializer.save()

    # Fetch or create invitation stats
    invitation_stats, _ = InvitationStats.objects.get_or_create(user=request.user)
    stats_serializer = InvitationStatsSerializer(invitation_stats)

    return Response(
        {
            "status": "success",
            "message": "Invitation link generated and stats fetched successfully.",
            "data": stats_serializer.data,
        },
        status=status.HTTP_201_CREATED,
    )
