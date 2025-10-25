from django.db import transaction
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import status
from invitations.models import Invitation, InvitationStats


@transaction.atomic
def handle_invitation_confirm(request, link_code):
    """
    Handles confirmation of a guest's registration via an invitation link.
    Ensures one-time use, checks expiration, and updates stats.
    """
    try:
        invitation = (
            Invitation.objects
            .select_for_update()
            .select_related("user")
            .get(link_code=link_code)
        )
    except Invitation.DoesNotExist:
        return Response(
            {"status": "error", "message": "Invalid invitation code."},
            status=status.HTTP_404_NOT_FOUND
        )

    # ❌ Expired invitation
    if invitation.is_expired:
        invitation.mark_as_expired()
        return Response(
            {"status": "error", "message": "This invitation has expired."},
            status=status.HTTP_400_BAD_REQUEST
        )

    # ❌ Already used
    if invitation.registered or invitation.usage_count >= 1:
        return Response(
            {"status": "error", "message": "This invitation has already been used."},
            status=status.HTTP_400_BAD_REQUEST
        )

    # ✅ Optional guest info updates
    allowed_fields = ["guest_name", "company_name", "personal_message"]
    for field in allowed_fields:
        if field in request.data and request.data[field]:
            setattr(invitation, field, request.data[field].strip())

    # ✅ Mark as registered
    invitation.registered = True
    invitation.registered_at = timezone.now()
    invitation.usage_count = 1
    invitation.status = "active"
    invitation.save(
        update_fields=[
            "guest_name",
            "company_name",
            "personal_message",
            "registered",
            "registered_at",
            "updated_at",
            "usage_count",
            "status",
        ]
    )

    # ✅ Update exhibitor stats
    stats, _ = InvitationStats.objects.select_for_update().get_or_create(id =1)
    stats.registered_visitors += 1
    stats.save(update_fields=["registered_visitors"])

    return Response(
        {
            "status": "success",
            "message": "Registration confirmed successfully.",
            "data": {
                "guest_name": invitation.guest_name,
                "company_name": invitation.company_name,
                "personal_message": invitation.personal_message,
                "registered_at": invitation.registered_at,
                "usage_count": invitation.usage_count,
                "status": invitation.status,
            },
        },
        status=status.HTTP_200_OK
    )
