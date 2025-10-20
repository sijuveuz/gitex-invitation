from rest_framework.response import Response
from rest_framework import status
from invitations.serializers import InvitationLinkRegisterSerializer


def handle_register_from_link(request):
    """
    Handles guest registration from an invitation link.
    Validates input, creates the registration record,
    and returns registration details on success.
    """
    serializer = InvitationLinkRegisterSerializer(data=request.data)

    if not serializer.is_valid():
        return Response(
            {"status": "error", "message": "Invalid data", "errors": serializer.errors},
            status=status.HTTP_400_BAD_REQUEST,
        )
    
    usage = serializer.save()

    return Response(
        {
            "status": "success",
            "message": "Registration successful.",
            "data": {
                "guest_name": usage.guest_name,
                "guest_email": usage.guest_email,
                "company_name": usage.company_name,
                "registered_at": usage.registered_at,
            },
        },
        status=status.HTTP_201_CREATED,
    )
