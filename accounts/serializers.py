from rest_framework import serializers
from django.contrib.auth import get_user_model
from .utils.validators import validate_name, validate_password_strength

User = get_user_model()


class UserRegistrationSerializer(serializers.ModelSerializer):
    """
    Serializer for registering a new user.
    """
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        fields = ("first_name", "last_name", "email", "password")

    def validate_email(self, value):
        """Ensure the email is unique."""
        if User.objects.filter(email=value).exists():
            raise serializers.ValidationError("Email already exists.")
        return value
    
    def validate(self, data):
        validate_name(data.get("first_name", ""), "first_name")
        validate_name(data.get("last_name", ""), "last_name")
        validate_password_strength(data.get("password", ""))
        return data

class UserLoginSerializer(serializers.Serializer):
    """
    Serializer for user login.
    """
    email = serializers.EmailField()
    password = serializers.CharField(write_only=True)


class UserDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'email']