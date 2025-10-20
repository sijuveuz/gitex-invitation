from django.db import transaction
from invitations.models import InvitationStats
from rest_framework_simplejwt.tokens import RefreshToken
from accounts.models import User


@transaction.atomic
def create_user_and_tokens(validated_data):
    """
    Create user(registration) and default invitation stats in a single atomic transaction.
    Returns user instance and JWT tokens.
    """

    # Check if user already exists 
    email = validated_data["email"]
    if User.objects.filter(email=email).exists():
        raise ValueError("A user with this email already exists.")

    with transaction.atomic():
        user = User.objects.create_user(
            email=email,
            first_name=validated_data.get("first_name", ""),
            last_name=validated_data.get("last_name", ""),
            password=validated_data["password"]
        )

        # Ensure InvitationStats exists or create a new one
        InvitationStats.objects.get_or_create(user=user)

    # Create JWT tokens
    refresh = RefreshToken.for_user(user)
    tokens = {
        "refresh": str(refresh),
        "access": str(refresh.access_token)
    }

    return user, tokens
