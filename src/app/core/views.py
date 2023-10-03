from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response


@api_view(["GET"])
def getHealth(request):
    return Response({"status": "alive and Kickin"}, status=status.HTTP_200_OK)
