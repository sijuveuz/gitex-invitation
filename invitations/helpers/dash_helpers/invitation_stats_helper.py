from rest_framework.response import Response
from rest_framework import status
from invitations.serializers import InvitationStatsSerializer
from invitations.models import InvitationStats



def get_user_invitation_stats(user):
    """
    Fetch or create invitation stats for a given user.
    """
    stats, _ = InvitationStats.objects.get_or_create(user=user)
    return stats



def handle_invitation_stats_request(request):
    """
    Fetches and returns invitation statistics for the authenticated user.
    """
    try:
        stats = get_user_invitation_stats(request.user)

        if not stats:
            return Response(
                {
                    "status": "error",
                    "message": "Invitation stats not found for this user.",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        serializer = InvitationStatsSerializer(stats)
        return Response(
            {
                "status": "success",
                "message": "Invitation stats fetched successfully.",
                "data": serializer.data,
            },
            status=status.HTTP_200_OK,
        )

    except Exception:
        return Response(
            {
                "status": "error",
                "message": "Unable to fetch invitation stats. Please try again later.",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )
