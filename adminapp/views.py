from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from adminapp.models import DuplicateRecord
from adminapp.serializers import DuplicateRecordSerializer

class DuplicateRecordListView(APIView):
    """
    Returns duplicate records for a given job_id.
    Supports filtering by ticket_type or email.
    """

    def get(self, request):
        job_id = request.query_params.get("job_id")
        email = request.query_params.get("email")
        ticket = request.query_params.get("ticket")

        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        qs = DuplicateRecord.objects.filter(job_id=job_id)

        if email:
            qs = qs.filter(guest_email__icontains=email)
        if ticket:
            qs = qs.filter(ticket_type__name__icontains=ticket)

        serializer = DuplicateRecordSerializer(qs, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
