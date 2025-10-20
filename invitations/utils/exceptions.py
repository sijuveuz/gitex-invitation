from rest_framework.exceptions import ValidationError as DRFValidationError
from django.core.exceptions import ValidationError as DjangoValidationError

def extract_validation_message(error):
    """
    Normalizes Django and DRF ValidationErrors into a plain string message.
    """
    if isinstance(error, DRFValidationError):
        detail = error.detail
        if isinstance(detail, dict):
            first_field = next(iter(detail))
            msg = detail[first_field] 
            if isinstance(msg, (list, tuple)):
                return msg[0]
            return str(msg)
        elif isinstance(detail, (list, tuple)):
            return detail[0]
        return str(detail)

    if isinstance(error, DjangoValidationError):
        if hasattr(error, "messages"):
            return error.messages[0] if error.messages else str(error)
        if hasattr(error, "message_dict"):
            first_key = next(iter(error.message_dict))
            return error.message_dict[first_key][0]
        return str(error)

    # Fallback for unexpected errors
    return str(error)
