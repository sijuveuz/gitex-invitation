import hashlib

def make_dedup_key(email, ticket_type=None, scope="ticket"):
    email_norm = email.lower().strip()

    # if scope == "global":
    #     return f"dedup:{user_id}:{email_norm}"

    if scope == "ticket" and ticket_type:
        ticket_norm = ticket_type.lower().strip()
        return f"dedup:{ticket_norm}:{email_norm}"

    # If no uniqueness required, just return None or skip
    return None


def resolve_dedup_scope(ticket_name, ticket_cache):
    """
    Decide which deduplication scope applies dynamically.
    Returns one of: 'global', 'ticket', or 'none'.
    """

    # 2️⃣ Ticket-wise uniqueness check
    ticket_type_norm = (ticket_name or "").lower().strip()
    ticket_info = ticket_cache.get(ticket_type_norm)

    print('resolve_dedup_scope', ticket_type_norm, ticket_info)
    if ticket_info and ticket_info.get("enforce_unique_email"):
        # This ticket type enforces unique emails
        return "ticket"

    # 3️⃣ Otherwise, no uniqueness enforced
    return "none"
