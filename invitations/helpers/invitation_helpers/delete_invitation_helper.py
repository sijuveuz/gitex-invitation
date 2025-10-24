from django.db import transaction
from rest_framework.exceptions import ValidationError
from invitations.models import Invitation, InvitationStats


def delete_invitation_helper(user, invitation_id):
    """
    Deletes or soft-deletes an invitation based on usage.
    - If usage_count > 0 → soft delete (mark inactive)
    - If usage_count == 0 → hard delete
    Updates InvitationStats accordingly.
    """
    with transaction.atomic():
        try:
            invitation = Invitation.objects.select_for_update().get(id=invitation_id)
        except Invitation.DoesNotExist:
            raise ValidationError({"detail": "Invitation not found."})


        stats = InvitationStats.objects.select_for_update().get(id=1)

        # 🟡 CASE 1: SOFT DELETE (already used or partially used)
        if invitation.usage_count > 0:
            invitation.link_is_active = False
            invitation.save(update_fields=["link_is_active", "updated_at"])

            # Reduce remaining invitations only by unused part
            unused_slots = max(invitation.usage_limit - invitation.usage_count, 0)
            if unused_slots > 0:
                stats.remaining_invitations = max(stats.remaining_invitations + unused_slots, 0)
                stats.save(update_fields=["remaining_invitations"])

            action = "soft_deleted"

        # CASE 2: HARD DELETE (never used)
        else:
            invitation.delete()

            # Update stats
            stats.generated_invitations = max(stats.generated_invitations - invitation.usage_limit, 0)
            stats.remaining_invitations = min(
                stats.remaining_invitations + invitation.usage_limit,
                stats.allocated_invitations
            )
            stats.save(update_fields=["generated_invitations", "remaining_invitations"])

            action = "hard_deleted"

    return {"action": action, "invitation_id": invitation_id}
