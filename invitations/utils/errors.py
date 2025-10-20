def get_error_message(error):
    """
    Extracts a clean string message from Django/DRF ValidationError or any Exception.
    """
    if hasattr(error, "detail"):
        detail = error.detail
        if isinstance(detail, dict):
            # take first key's message
            first_val = next(iter(detail.values()))
            if isinstance(first_val, list):
                return str(first_val[0])
            return str(first_val)
        elif isinstance(detail, list):
            return str(detail[0])
        else:
            return str(detail)
    return str(error)
