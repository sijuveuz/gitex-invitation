from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status

from .serializers import UserRegistrationSerializer, UserLoginSerializer
from .helpers.registration_helper import create_user_and_tokens
from .helpers.login_helper import authenticate_user


class UserRegistrationView(APIView):
    """
    Handles user registration using email and password.
    Returns user details + JWT tokens after successful registration.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = UserRegistrationSerializer(data=request.data)

        if serializer.is_valid():
            user, tokens = create_user_and_tokens(serializer.validated_data)
            
            response_data = {
                "status": "success",
                "message": "User registered successfully.",
                "data": {
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                    },
                    "tokens": tokens,
                },
            }
            return Response(response_data, status=status.HTTP_201_CREATED)

        return Response(
            {"status": "error", "errors": serializer.errors},
            status=status.HTTP_400_BAD_REQUEST,
        )


class UserLoginView(APIView):
    """
    Handles user login using email and password.
    Returns access and refresh JWT tokens on success.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = UserLoginSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        email = serializer.validated_data["email"]
        password = serializer.validated_data["password"]

        try:
            user, tokens = authenticate_user(email, password)
        except Exception as e:
            return Response(
                {"status": "error", "message": str(e)},
                status=status.HTTP_401_UNAUTHORIZED
            )

        response_data = {
            "status": "success",
            "message": "Login successful.",
            "data": {
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                },
                "tokens": tokens,
            },
        }
        return Response(response_data, status=status.HTTP_200_OK)