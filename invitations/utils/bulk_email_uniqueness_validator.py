from adminapp.models import TicketType, InvitationSettings

def load_ticket_email_validation_context():
    global_unique_enabled = (
        InvitationSettings.objects.first().enforce_global_unique
        if InvitationSettings.objects.exists()
        else False
    )
    ticket_cache = {
        t.name.lower(): {
            "name": t.name,
            "enforce_unique_email": t.enforce_unique_email,
        }
        for t in TicketType.objects.all()
    }
    return global_unique_enabled, ticket_cache
