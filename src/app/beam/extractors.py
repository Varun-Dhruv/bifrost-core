import apache_beam as beam
from core.constants import *


def ExtractorFactory(method, type):
    pass


def mongodb_extractor(pipeline, path):
    lines = pipeline | "ReadMyDatabase" >> beam.io.ReadFromMongoDB(
        uri=path["url"], db=path["database"], coll=path["collection"]
    )
    lines = lines | "RemoveObjectId" >> beam.Map(
        lambda line: {k: v for k, v in line.items() if k != "_id" or k != "__v"}
    )
    return lines


def file_extractor(pipeline, path):
    if path["type"] == "csv":
        lines = pipeline | "ReadFromDatabase" >> beam.io.ReadAllFromText(path["path"], skip_header_lines=1)

        lines = lines | "ConvertToDict" >> beam.Map(lambda line: dict(zip(path["fields"], line.split(","))))

        return lines
    if path["type"] == "txt":
        pass


def kafka_extractor(pipeline, path):
    pass
