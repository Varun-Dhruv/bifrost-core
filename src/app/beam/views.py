# Create your views here.
from beam.utils import check_config_validity
from rest_framework.decorators import api_view
from rest_framework.parsers import JSONParser
from rest_framework.response import Response

def check_valid_node(node):
    pass
    
def parse_graph(): # should return a set of jobs
    pass


"""
    source= {
        node:[t1,t2],
        node2:[t3,t4],
    }
    transforms={
        t1:[s1,s3],
        t2:[s2]
    }
    node 
    / \ 
   t1  t2
   |   |
   s1  s2 
"""


@api_view(["POST"])
def run(request):
    try:
        data = JSONParser().parse(request)["data"]
        # check config validity
        check_config_validity(data["pipeline"])
        # run_pipeline(data)
        return Response({"message": data})
    except Exception as e:
        return Response({"message": str(e)})
    
    
