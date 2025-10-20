from django.contrib.auth import authenticate
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework.exceptions import AuthenticationFailed

def authenticate_user(email, password):
    """
    Authenticate user with email and password.
    Returns (user, tokens) if successful.
    Raises AuthenticationFailed on invalid credentials.
    """
    user = authenticate(email=email, password=password)

    if user is None:
        raise AuthenticationFailed("Invalid email or password.")

    if not user.is_active:
        raise AuthenticationFailed("This account is inactive.")

    refresh = RefreshToken.for_user(user)
    tokens = {
        "refresh": str(refresh),
        "access": str(refresh.access_token)
    }

    return user, tokens
