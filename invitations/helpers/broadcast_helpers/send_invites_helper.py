from django.db import transaction
from rest_framework.exceptions import ValidationError
from invitations.models import Invitation, InvitationLinkUsage, InvitationStats


from django.db import transaction
from rest_framework.exceptions import ValidationError

def broadcast_invitation_helper(user, data):
    """
    Handles broadcasting invitations depending on source type.
    """
    invitation_id = data.get("invitation_id")
    source_type = data.get("source_type")

    with transaction.atomic():
        try:
            invitation = Invitation.objects.select_for_update().get(id=invitation_id, user=user)
        except Invitation.DoesNotExist:
            raise ValidationError({"detail": "Invitation not found."})

        # --- LINK INVITATION FLOW ---
        if source_type == "link":
            guest_name = data.get("guest_name", "").strip()
            guest_email = data.get("guest_email", "").lower().strip()

            if not guest_name or not guest_email:
                raise ValidationError({"detail": "Guest name and email are required for link invitations."})

            usage, created = InvitationLinkUsage.objects.get_or_create(
                link=invitation,
                guest_email=guest_email,
                defaults={
                    "guest_name": guest_name,
                    "company_name": data.get("company_name"),
                },
            )

            if not created:
                if usage.registered:
                    raise ValidationError({"detail": f"Guest '{guest_email}' has already registered via this link."})
                else:
                    raise ValidationError({"detail": f"Broadcast already done for '{guest_email}'."})

            print(f"📧 Queued link invitation email to {guest_email}")

            return {
                "action": "link_usage_created",
                "invitation_id": str(invitation.id),
                "guest_email": guest_email,
            }

        # --- PERSONAL / BULK FLOW ---
        elif source_type in ["personal", "bulk"]:
            editable_fields = ["guest_name", "company_name", "personal_message", "expire_date", "ticket_type"]
            for field in editable_fields:
                if field in data:
                    setattr(invitation, field, data[field])

            invitation.save(update_fields=editable_fields + ["updated_at"])
            print(f"📧 Queued resend email to {invitation.guest_email}")

            return {
                "action": "invitation_updated",
                "invitation_id": str(invitation.id),
                "guest_email": invitation.guest_email,
            }

        else:
            raise ValidationError({"detail": "Invalid source_type provided."})

