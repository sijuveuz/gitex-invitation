from rest_framework import serializers
from adminapp.models import DuplicateRecord

class DuplicateRecordSerializer(serializers.ModelSerializer):
    ticket_name = serializers.CharField(source="ticket_type.name", read_only=True)
    user_email = serializers.EmailField(source="user.email", read_only=True)

    class Meta:
        model = DuplicateRecord
        fields = [
            "id",
            "user_email",
            "job_id",
            "guest_email",
            "ticket_name",
            "scope",
            "detection_source",
            "reason",
            "created_at",
        ]
