from rest_framework import serializers
from django.utils import timezone
from decouple import config
from django.db import transaction
from uuid import uuid4

from adminapp.models import TicketType
from invitations.utils.email_uniqueness_validator import check_email_uniqueness
from .models import Invitation
from invitations.models import (
    InvitationStats, 
    BulkUploadJob, InvitationLinkUsage
                                )
from accounts.serializers import UserDetailsSerializer


class InvitationStatsSerializer(serializers.ModelSerializer):
    """
    Serializer for InvitationStats model.
    """
    class Meta:
        model = InvitationStats
        fields = [
            "allocated_invitations",
            "generated_invitations",
            "remaining_invitations",
            "registered_visitors",
        ]


class TicketTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = TicketType
        fields = ["id", "name", "description", "is_active"]


class PersonalizedInvitationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Invitation
        fields = (
            "guest_name", "guest_email", "company_name",
            "personal_message", "ticket_type", "expire_date",
            "link_is_active", "link_limit_reached"
        )

    def validate_guest_name(self, value):
        if len(value.strip()) < 2 or not all(c.isalpha() or c.isspace() for c in value):
            raise serializers.ValidationError("Please enter guest's full name") 
        return value

    def validate_guest_email(self, value):
        return value

    def validate_expire_date(self, value):
        if value <= timezone.now().date():
            raise serializers.ValidationError("Expiry date must be a future date")
        return value

    def validate_ticket_type(self, value):
        if not TicketType.objects.filter(id=value.id, is_active=True).exists():
            raise serializers.ValidationError("Selected ticket type is invalid")
        return value


class BulkUploadCreateSerializer(serializers.Serializer):
    file = serializers.FileField()

class BulkUploadJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = BulkUploadJob
        fields = ("id", "file_name", "status", "total_count", "valid_count", "invalid_count", "preview_data", "sample_errors", "created_at", "updated_at", 'uploaded_file', 'expire_date', 'default_personal_message')


class InvitationListSerializer(serializers.ModelSerializer):
    invite_type = serializers.SerializerMethodField()
    # registered = serializers.IntegerField(source="usage_count", read_only=True)
    link_limit = serializers.IntegerField(source="usage_limit", read_only=True)
    ticket_type = TicketTypeSerializer()
    user = UserDetailsSerializer()

    class Meta:
        model = Invitation
        fields = [
            "id",
            "user" ,
            "guest_name",
            "guest_email",
            "invite_type",
            "expire_date",
            "link_limit",
            "ticket_type",
            "registered",
            "usage_count",
            "invitation_url",   
            "status",
            "source_type",
            "created_at",
            "link_limit_reached",
            "link_is_active"
        ]

    def get_invite_type(self, obj):
        if obj.source_type == "link":
            return "Invitation Link"
        elif obj.source_type == "personal":
            return "Personalized"
        elif obj.source_type == "bulk":
            return "Bulk Upload"
        return "Unknown"


class InvitationDetailSerializer(serializers.ModelSerializer):
    ticket_type = TicketTypeSerializer()
    user = UserDetailsSerializer()
    class Meta:
        model = Invitation
        fields = (
            "user",
            "guest_name",
            "guest_email",
            "company_name",
            "personal_message",
            "ticket_type",
            "expire_date",
            "status",
            "registered",
            "invitation_url",
            "link_limit_reached"
        )




class InvitationLinkGenerateSerializer(serializers.ModelSerializer):
    #Custom field not present in the model
    links_needed = serializers.IntegerField(write_only=True, min_value=1)

    class Meta:
        model = Invitation
        fields = ["guest_name", "ticket_type", "expire_date", "usage_limit", "links_needed"]

    def create(self, validated_data):
        user = self.context["request"].user
        base_url = config("FRONTEND_URL", "http://178.18.253.63:3010/invite/register")
        links_needed = validated_data.pop("links_needed", 1)

        stats, _ = InvitationStats.objects.select_for_update().get_or_create(id=1)

        with transaction.atomic():
            stats.refresh_from_db()

            if stats.remaining_invitations < links_needed:
                raise serializers.ValidationError({
                    "detail": f"Not enough invitations left. You have only {stats.remaining_invitations} remaining."
                })

            invitations = []
            for _ in range(links_needed):
                link_code = uuid4()
                invitations.append(
                    Invitation(
                        user=user,
                        source_type="link",
                        link_code=link_code,
                        invitation_url=f"{base_url}/{link_code}",
                        **validated_data,
                    )
                )
                stats.generated_invitations += links_needed

            Invitation.objects.bulk_create(invitations, batch_size=1000)

            stats.update_remaining()

        return {
            "total_created": links_needed,
            "remaining": stats.remaining_invitations,
        }


class InvitationLinkRegisterSerializer(serializers.Serializer):
    link_code = serializers.UUIDField()
    guest_name = serializers.CharField(max_length=255)
    guest_email = serializers.EmailField()
    company_name = serializers.CharField(max_length=255, required=False, allow_blank=True)

    def validate(self, data):
        try:
            invitation = Invitation.objects.get(link_code=data["link_code"], source_type="link")
            email = data["guest_email"].lower().strip()
            ticket_type = invitation.ticket_type
        except Invitation.DoesNotExist:
            raise serializers.ValidationError({"detail": "Invalid or non-existent invitation link."})

        if invitation.is_expired:
            raise serializers.ValidationError({"detail": "This invitation link has expired."})

        if invitation.usage_count >= invitation.usage_limit:
            raise serializers.ValidationError({"detail": "This invitation link has reached its usage limit."})

        is_valid, msg, _ = check_email_uniqueness(email, ticket_type)
        if not is_valid:
            raise serializers.ValidationError({"detail": msg})
        # check existing usage for same email & same ticket type
        existing_usage = (
            InvitationLinkUsage.objects
            .filter(link=invitation, guest_email=data["guest_email"])
            .select_related("link")
            .first()
        ) 

        if existing_usage:
            if existing_usage.registered:
                # already fully registered
                raise serializers.ValidationError({"detail": "You have already registered using this link."})
            else:
                # partially created earlier but not registered yet
                data["existing_usage"] = existing_usage

        data["invitation"] = invitation
        return data

    def create(self, validated_data):
        from django.db.models import F
        invitation = validated_data.pop("invitation")
        validated_data.pop("link_code", None)

        existing_usage = validated_data.pop("existing_usage", None)

        if existing_usage:
            # Update existing record to mark registration complete
            existing_usage.guest_name = validated_data.get("guest_name", existing_usage.guest_name)
            existing_usage.company_name = validated_data.get("company_name", existing_usage.company_name)
            existing_usage.registered = True
            existing_usage.save(update_fields=["guest_name", "company_name", "registered"])

            # Update counters and stats
            invitation.usage_count = F("usage_count") + 1
            invitation.save(update_fields=["usage_count"])

            stats, _ = InvitationStats.objects.select_for_update().get_or_create(id=1)
            stats.registered_visitors = F("registered_visitors") + 1
            stats.save(update_fields=["registered_visitors"])

            return existing_usage

        usage = InvitationLinkUsage.objects.create(link=invitation, **validated_data)
        usage.registered = True
        usage.save(update_fields=["registered"])

        # Increment invitation usage count atomically
        invitation.usage_count = F("usage_count") + 1
        invitation.save(update_fields=["usage_count"])

        # Update stats
        stats, _ = InvitationStats.objects.select_for_update().get_or_create(id=1)
        stats.registered_visitors = F("registered_visitors") + 1
        stats.save(update_fields=["registered_visitors"])

        return usage


class InvitationLinkUsageSerializer(serializers.ModelSerializer):
    class Meta:
        model = InvitationLinkUsage
        fields = ["guest_name", "guest_email", "company_name", "registered_at", "registered"]


class InvitationDetailSerializerById(serializers.ModelSerializer):
    usages = serializers.SerializerMethodField()
    ticket_type = TicketTypeSerializer()

    class Meta:
        model = Invitation
        fields = [
            "id", "guest_name", "guest_email", "company_name",
            "personal_message", "ticket_type", "expire_date",
            "source_type", "link_code", "usage_limit", "usage_count",
            "invitation_url", "status", "created_at", "updated_at",
            "registered_at", "registered", "usages", "link_is_active",
            "link_limit_reached"
        ]

    def get_usages(self, obj):
        if obj.source_type == "link":
            usages = obj.usages.all()  # related_name='usages'
            return InvitationLinkUsageSerializer(usages, many=True).data
        return None
