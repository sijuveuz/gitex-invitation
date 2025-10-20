# accounts/utils/validators.py
import re
from rest_framework.exceptions import ValidationError


def validate_name(name: str, field_name: str):
    """Ensure first/last name contains only letters and spaces."""
    if not name.replace(" ", "").isalpha():
        raise ValidationError({field_name: f"{field_name.capitalize()} should contain only letters."})


def validate_password_strength(password: str):
    """Ensure password meets minimum strength."""
    if len(password) < 8:
        raise ValidationError({"password": "Password must be at least 8 characters long."})
    if not re.search(r"[A-Z]", password):
        raise ValidationError({"password": "Password must contain at least one uppercase letter."})
    if not re.search(r"[a-z]", password):
        raise ValidationError({"password": "Password must contain at least one lowercase letter."})
    if not re.search(r"\d", password):
        raise ValidationError({"password": "Password must contain at least one number."})
