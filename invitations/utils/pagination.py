from rest_framework.pagination import PageNumberPagination


class StandardResultsSetPagination(PageNumberPagination):
    """
    Default pagination class for API views.
    Allows dynamic page size via query params.
    """
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 100
