from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from django.conf import settings
from django.http import FileResponse, Http404
import os

from .serializers import (
                          TicketTypeSerializer)
from .helpers.ticket_helper import get_active_ticket_types
from rest_framework.exceptions import ValidationError  
from .helpers.bulk_helpers.fetch_bulk_rows_helper import handle_bulk_rows_request
from .helpers.bulk_helpers.bulk_add_row_helper import handle_bulk_add_row
from .helpers.bulk_helpers.bulk_row_patch_helper import handle_bulk_row_patch
from .helpers.bulk_helpers.bulk_clear_preview_helper import handle_bulk_clear_preview
from .helpers.bulk_helpers.bulk_confirm_helper import handle_bulk_confirm_request
from .helpers.bulk_helpers.bulk_row_delete_helper import handle_bulk_row_delete
from .helpers.bulk_helpers.bulk_job_status_helper import handle_bulk_job_status
from .helpers.invitation_helpers.invitation_list_helper import handle_invitation_list
from .helpers.invitation_helpers.invitation_link_generate_helper import handle_invitation_link_generate
from .helpers.invite_confirmaion.register_from_link_view import handle_register_from_link
from .helpers.invitation_helpers.generate_invitation_link_details_helper import handle_generate_invitation_link_details
from .helpers.invitation_helpers.invitation_detail_by_id_helper import handle_invitation_detail_by_id
from .helpers.dash_helpers.invitation_edit_helper import handle_invitation_edit
from .helpers.personal_invite_helpers.personal_invite_helper import handle_send_personal_invitation
from .helpers.bulk_helpers.bulk_upload_helper import handle_bulk_upload
from .helpers.dash_helpers.invitation_stats_helper import handle_invitation_stats_request


class InvitationStatsView(APIView):
    """
    GET /api/invitations/stats/
    Retrieves the logged-in user's invitation statistics.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        return handle_invitation_stats_request(request)


class TicketTypeListView(APIView):
    """
    GET: Retrieve all available ticket types
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        ticket_types = get_active_ticket_types()
        serializer = TicketTypeSerializer(ticket_types, many=True)
        response_data = {
            "status": "success",
            "message": "Ticket types fetched successfully.",
            "data": serializer.data,
        }
        return Response(response_data, status=status.HTTP_200_OK)
    

class SendPersonalInvitationView(APIView):
    """
    POST /api/invitations/personal/send/
    Handles creating and sending a personal (one-to-one) invitation.
    """
    permission_classes = [IsAuthenticated]
 
    def post(self, request):
        return handle_send_personal_invitation(request)


class BulkUploadView(APIView):
    """
    POST /api/invitations/bulk/upload/
    Handles bulk CSV upload, creates a job, and triggers async validation.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        return handle_bulk_upload(request)


class BulkRowsView(APIView):
    """
    Retrieves paginated, filtered, and validated preview rows
    for a specific bulk upload job from Redis.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id):
        return handle_bulk_rows_request(request, job_id)



class BulkClearPreviewView(APIView):
    """
    Clears all preview rows and stats from Redis for the given bulk upload job.
    """
    permission_classes = [IsAuthenticated]

    def delete(self, request, job_id):
        return handle_bulk_clear_preview(request, job_id)




class BulkAddRowView(APIView):
    """
    Adds a new guest row to a bulk upload job, validates the data,
    and updates the job stats in Redis.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request, job_id):
        return handle_bulk_add_row(request, job_id)



class BulkRowPatchView(APIView):
    """
    Updates a specific row in a bulk upload job with existing guest details,
    revalidates it, and saves the updated data to Redis.
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, job_id, row_number):
        return handle_bulk_row_patch(request, job_id, row_number)


class BulkConfirmView(APIView):
    """
    Confirms a bulk upload job after validation, checks user quota,
    and starts background processing of invitations - invite only for valid rows.
    """
    permission_classes = [IsAuthenticated]

    @transaction.atomic
    def post(self, request, job_id):
        return handle_bulk_confirm_request(request, job_id)


class BulkRowDeleteView(APIView):
    """
    Deletes a specific preview row from a bulk upload job in Redis
    and updates job statistics accordingly.
    """
    permission_classes = [IsAuthenticated]

    def delete(self, request, job_id, row_number):
        return handle_bulk_row_delete(request, job_id, row_number)


class BulkJobStatusView(APIView):
    """
    Retrieves the current processing status and summary
    statistics for a bulk upload job.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id):
        return handle_bulk_job_status(request, job_id)



class InvitationListView(APIView):
    """
    Returns a paginated and filtered list of active invitations
    belonging to the authenticated user.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        return handle_invitation_list(request)


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from invitations.helpers.invitation_helpers.invitation_detail_helper import handle_invitation_detail
 

class InvitationDetailView(APIView):
    """
    Retrieves detailed invitation data by unique link code (UUID).
    Handles invalid or expired invitations gracefully.
    """
    permission_classes = [AllowAny]
 
    def get(self, request, link_code):
        return handle_invitation_detail(request, link_code)



from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from invitations.helpers.invite_confirmaion.invitation_confirm_helper import handle_invitation_confirm


class InvitationConfirmView(APIView):
    """
    Confirms guest registration using a unique invitation link.
    Ensures an invitation can be used only once.
    """
    permission_classes = [AllowAny]

    def post(self, request, link_code):
        return handle_invitation_confirm(request, link_code)




class GenerateInvitationLinkView(APIView): 
    """
    Allows exhibitors to generate a new invitation link
    and returns updated invitation stats.
    """
    permission_classes = [IsAuthenticated]

    @transaction.atomic()
    def post(self, request):
        return handle_invitation_link_generate(request)


 
class RegisterFromLinkView(APIView):
    """
    Allows guests to register using a shared invitation link.
    """
    permission_classes = [AllowAny]

    @transaction.atomic
    def post(self, request):
        return handle_register_from_link(request)


class GenerateInvitationLinkDetailsView(APIView):
    """
    Retrieves details of an invitation generated via link using its UUID.
    """
    permission_classes = [AllowAny]

    def get(self, request, link_code): 
        return handle_generate_invitation_link_details(request, link_code)


class InvitationDetailByIdView(APIView):
    """
    Retrieves detailed information of a specific invitation by ID.
    Automatically marks it as expired if past its expiration date.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, pk):
        return handle_invitation_detail_by_id(request, pk)
    

class InvitationEditView(APIView):
    """
    PATCH /api/invitations/<uuid:pk>/edit/
    Allows exhibitors to edit invitation fields:
    guest_name, company_name, ticket_type, and expire_date.
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, pk):
        return handle_invitation_edit(request, pk)
    
        
from invitations.helpers.invitation_helpers.delete_invitation_helper import delete_invitation_helper
class InvitationDeleteView(APIView):
    """
    DELETE /api/invitations/<uuid:pk>/delete/
    Performs soft or hard delete based on invitation usage.
    """
    permission_classes = [IsAuthenticated]

    def delete(self, request, pk):
        try:
            result = delete_invitation_helper(request.user, pk)
            return Response(
                {
                    "status": "success",
                    "message": f"Invitation {result['action']} successfully.",
                    "data": result
                },
                status=status.HTTP_200_OK
            )
        except Exception as e:
            return Response(
                {"status": "error", "message": str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

from invitations.helpers.broadcast_helpers.send_invites_helper import broadcast_invitation_helper


class BroadcastInvitationView(APIView):
    """
    POST /api/invitations/broadcast/
    Handles sending invitations for different sources (link, personal, bulk)
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            result = broadcast_invitation_helper(request.user, request.data)
            return Response(
                {
                    "status": "success",
                    "message": "Invitation broadcast processed successfully.",
                    "data": result,
                },
                status=status.HTTP_200_OK,
            )

        except ValidationError as e:
            # Extract first readable message from DRF's ErrorDetail
            detail = e.detail
            if isinstance(detail, dict) and "detail" in detail:
                message = detail["detail"]
            elif isinstance(detail, list) and detail:
                message = detail[0]
            else:
                message = str(detail)
            return Response(
                {"status": "error", "message": message},
                status=status.HTTP_400_BAD_REQUEST,
            )

        except Exception as e:
            return Response(
                {"status": "error", "message": str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )
            




import uuid
from rest_framework.views import APIView
from rest_framework.response import Response 
from invitations.tasks.export_invitations_task import export_invitations_task

class InvitationExportStartView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        export_format = request.data.get("format", "csv").lower()
        user = request.user
        job_id = str(uuid.uuid4())

        export_invitations_task.delay(user.id, export_format, job_id)

        return Response({
            "job_id": job_id,
            "message": f"Export started in {export_format} format.",
        })
    
from invitations.utils.redis_utils import get_redis

class InvitationExportStatusView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id):
        r = get_redis()
        result = r.get(str(job_id))
        if not result:
            return Response({"status": "processing"})
        # file_url = result.decode("utf-8")
        return Response({"status": "ready", "data": result})
    

class InvitationExportDownloadView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, filename):
        file_path = os.path.join(settings.MEDIA_ROOT, "exports", filename)
        if not os.path.exists(file_path):
            raise Http404("File not found")
        response = FileResponse(open(file_path, "rb"), as_attachment=True)
        return response
    


