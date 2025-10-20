from django.core.cache import cache
from ..models import TicketType

def get_active_ticket_types():
    # key = "ticket_types_active_v1"
    # data = cache.get(key)
    # if data is None:
    qs = TicketType.objects.filter(is_active=True).order_by("id").values("id", "name")
    data = list(qs)
        # cache.set(key, data, timeout=60*60)  # 1 hour
    return data
