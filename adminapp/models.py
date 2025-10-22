from django.db import models

class TicketType(models.Model):
    """
    Represents available ticket types for invitations (Visitor, VIP, etc.)
    """
    name = models.CharField(max_length=50, unique=True, db_index=True)
    description = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(default=True)
    enforce_unique_email = models.BooleanField(default=False)  # ticket-level uniqueness
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return  f"{self.name} Unique: {self.enforce_unique_email}"
    

class InvitationSettings(models.Model):
    """
    Global invitation configuration.
    Controls whether guest emails must be unique globally.
    """
    enforce_global_unique = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Global Unique: {self.enforce_global_unique}"

