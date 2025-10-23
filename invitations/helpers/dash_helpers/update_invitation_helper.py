# from invitations.models import Invitation
# from django.utils import timezone


# def update_invitation_details(user, pk, data):
#     """
#     Updates editable fields (guest_name, company_name, ticket_type, expire_date)
#     for a specific invitation belonging to the user.
#     Performs permission checks and validation.
#     """

#     try:
#         invitation = Invitation.objects.get(id=pk, user=user)
#     except Invitation.DoesNotExist:
#         raise ValueError("Invitation not found.")

#     # ✅ Only editable fields
#     editable_fields = ["guest_name", "company_name", "ticket_type", "expire_date"]
#     for field in editable_fields:
#         if field in data and data[field] not in [None, ""]:
#             setattr(invitation, field, data[field])

#     # ✅ Update timestamp
#     invitation.updated_at = timezone.now()
#     invitation.save(update_fields=[*editable_fields, "updated_at"])

#     return invitation
