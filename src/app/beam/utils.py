import csv
import json
from core.constants import *
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam.transformations import FormatAsJson
from beam.tasks import run_batch_job, run_stream_job


def check_config_validity(config):
    if not config:
        raise ValueError("Config is empty")
    if config.get("api_version") is not None and config["api_version"] not in API_VERSIONS:
        raise ValueError("api_version is missing")
    if config.get("kind") is not None and config["kind"] not in KIND:
        raise ValueError("kind is missing")
    if config.get("sources") is None:
        raise ValueError("No sources found")
    for source in config["sources"]:
        if source["type"] is None:
            raise ValueError("Source type is missing")
        if source["type"] not in SOURCE_TYPE:
            raise ValueError("Invalid source type")
        if source["method"] is None:
            raise ValueError("Source method is missing")
        if source["method"] not in BATCH_METHODS and source["method"] not in STREAM_METHODS:
            raise ValueError("Invalid source method")
        if source["path"] is None:
            raise ValueError("Source path is missing")
        # verify_path(source["path"], source["method"])
    return True


def prepare_jobs(config):
    kind = config["kind"]
    batch_jobs = []
    stream_jobs = []
    for source in config["sources"]:
        if source["method"] in BATCH_METHODS:
            batch_jobs.append(source)
        elif source["method"] in STREAM_METHODS:
            stream_jobs.append(source)
    if kind == "execute":
        if len(batch_jobs) > 0:
            for batch_job in batch_jobs:
                run_batch_job(batch_job)
        if len(stream_jobs) > 0:
            for stream_job in stream_jobs:
                run_stream_job(stream_job)
            return stream_jobs


def test_run():
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | "ReadMyFile" >> beam.io.ReadFromText("input.csv", skip_header_lines=1)
        parsed_data = lines | "ParseCSV" >> beam.Map(lambda line: next(csv.reader([line])))

        def to_json(row):
            keys = ["Name", "UID", "Address"]
            return json.dumps(dict(zip(keys, row)))

        parsed_data | "ToJSON" >> beam.Map(to_json) | "WriteJSON" >> beam.io.WriteToText("output.json")


# TO BE MOVED TO KNOWN LOCATION
TYPES_OF_SOURCES = [
    {
        "type": "batch",
        "method": "file",
        "file": "File",
    },
    {"type": "batch", "method": "mongodb", "path": {"url": "mongodb://localhost:27017", "collection": "test"}},
]
# TYPES_OF_TRANSFORMS = [
#     {
#         "type": "batch",
#         "method": "file",
#         "file": "File",
#     },
#     {"type": "batch", "method": "mongodb", "path": {"url": "mongodb://localhost:27017", "collection": "test"}},
# ]

# kafka_config = {
#     # Required connection configs for Kafka producer, consumer, and admin
#     "bootstrap.servers": "pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
#     "security.protocol": "SASL_SSL",
#     "sasl.mechanisms": "PLAIN",
#     "sasl.username": "TJZYKP53TGQBNFKD",
#     "sasl.password": "5SBsB9M0VpCjkKbKhgm1QoZL+Zz6NAzFXhMC7cdczMAczzkyV8WyxKc9naQXKFHC",
#     "group.id": "car-data-consumer",
#     "session.timeout.ms": 45000,
#     # "schema.registry.url":"https://psrc-vn38j.us-east-2.aws.confluent.cloud",
#     # "basic.auth.credentials.source":"USER_INFO",
#     # "basic.auth.user.info":{ "BVH7NEALOWHHTBJO" : "TeOSNkerFQHnigDwPXVkZrjfFa42cNiyczsROiwu9i7OEG/bGduyZkoBH2QcKVEO" }
# }
