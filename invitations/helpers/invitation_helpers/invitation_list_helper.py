from rest_framework.response import Response
from invitations.models import Invitation
from invitations.serializers import InvitationListSerializer
from invitations.helpers.invitation_helpers.invitation_filter_helper import apply_invitation_filters
from invitations.utils.pagination import StandardResultsSetPagination


def handle_invitation_list(request):
    """
    Fetches a paginated and filtered list of active invitations
    for the authenticated user.
    """
    user = request.user
    qs = Invitation.objects.filter(link_is_active=True).select_related("ticket_type")
    filtered_qs = apply_invitation_filters(qs, request.query_params)

    paginator = StandardResultsSetPagination()
    paginated_qs = paginator.paginate_queryset(filtered_qs, request)
    serializer = InvitationListSerializer(paginated_qs, many=True)

    return paginator.get_paginated_response({
        "status": "success",
        "message": "Invitations fetched successfully.",
        "data": serializer.data,
    })
