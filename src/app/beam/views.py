# Create your views here.
from beam.utils import check_config_validity, run_pipeline
from rest_framework.decorators import api_view
from rest_framework.parsers import JSONParser
from rest_framework.response import Response


@api_view(["POST"])
def run(request):
    try:
        data = JSONParser().parse(request)["data"]
        # check config validity
        check_config_validity(data)
        run_pipeline(data)
        return Response({"message": data})
    except Exception as e:
        return Response({"message": str(e)})
