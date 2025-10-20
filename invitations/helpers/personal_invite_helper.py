from django.db import transaction
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from invitations.models import Invitation, InvitationStats, TicketType
import uuid
from decouple import config
from invitations.utils.exceptions import extract_validation_message

def create_personal_invitation(user, data):
    """
    Atomic: checks quota, creates Invitation, updates stats.
    Returns (invitation, created_bool)
    """
    email = data["guest_email"].lower().strip()

    # try:
    try:
        ticket_type_obj = TicketType.objects.get(name=data["ticket_type"])
    except TicketType.DoesNotExist:
        raise ValidationError({
            "detail": f"Invalid ticket type '{data['ticket_type']}'. Please select a valid option."
        })

    existing = Invitation.objects.filter(
        user=user,
        guest_email=email,
        ticket_type=ticket_type_obj
    ).first()

    if existing:
        raise ValidationError({
            "detail": f"An invitation for '{email}' already exists with Ticket class '{data['ticket_type']}'."
        })

    with transaction.atomic():
        stats = InvitationStats.objects.select_for_update().get(user=user)

        if stats.remaining_invitations <= 0:
            raise ValidationError({
                "detail": "You have reached your invitation limit."
            })
        FRONTEND_INVITE_URL =  config('FRONTEND_INVITE_URL')
        invitation = Invitation.objects.create(
            user=user,
            guest_name=data["guest_name"],
            guest_email=email,
            company_name=data.get("company_name"),
            personal_message=data.get("personal_message", ""),
            ticket_type=ticket_type_obj, 
            expire_date=data["expire_date"],
            source_type="personal",
            # link_code=,
            # invitation_url=None,
            usage_limit=1,
            usage_count=0,
            status="active",
        )
        invitation.invitation_url = f'{FRONTEND_INVITE_URL}{str(invitation.link_code)}/'
        stats.generated_invitations += 1 
        stats.remaining_invitations = max(
            stats.allocated_invitations - stats.generated_invitations, 0
        )
        stats.save(update_fields=["generated_invitations", "remaining_invitations"])
        invitation.status = 'active'
        invitation.save()

    return invitation, True

from rest_framework.response import Response
from rest_framework import status
from rest_framework.exceptions import ValidationError
from invitations.serializers import PersonalizedInvitationSerializer

def handle_send_personal_invitation(request):
    """
    Handles the creation and sending of a personal invitation:
    - Validates the request data
    - Creates or updates an invitation record
    - Returns structured success/error responses
    """
    serializer = PersonalizedInvitationSerializer(data=request.data)

    try:
        serializer.is_valid(raise_exception=True)
        invitation, created = create_personal_invitation(
            request.user, serializer.validated_data
        )

        return Response(
            {
                "status": "success",
                "message": "Invitation created and queued for sending.",
                "data": {
                    "id": invitation.id,
                    "guest_email": invitation.guest_email,
                    "guest_name": invitation.guest_name,
                    "status": invitation.status,
                    "invitation_url": invitation.invitation_url,
                },
            },
            status=status.HTTP_200_OK,
        )

    except ValidationError as e:
        return Response(
            {"status": "error", "message": extract_validation_message(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_400_BAD_REQUEST,
        )
