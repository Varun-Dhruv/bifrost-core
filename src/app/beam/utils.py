import csv
import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

API_VERSIONS = ["v1", "v2"]
SOURCE_TYPE = ["stream", "batch"]
KIND = ["store", "execute", "schedule"]
BATCH_METHODS = ["mongodb", "file"]
STREAM_METHODS = ["kafka"]

kafka_config = {
    # Required connection configs for Kafka producer, consumer, and admin
    "bootstrap.servers": "pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "TJZYKP53TGQBNFKD",
    "sasl.password": "5SBsB9M0VpCjkKbKhgm1QoZL+Zz6NAzFXhMC7cdczMAczzkyV8WyxKc9naQXKFHC",
    "group.id": "car-data-consumer",
    # Best practice for higher availability in librdkafka clients prior to 1.7
    "session.timeout.ms": 45000,
    # "schema.registry.url":"https://psrc-vn38j.us-east-2.aws.confluent.cloud",
    # "basic.auth.credentials.source":"USER_INFO",
    # "basic.auth.user.info":{ "BVH7NEALOWHHTBJO" : "TeOSNkerFQHnigDwPXVkZrjfFa42cNiyczsROiwu9i7OEG/bGduyZkoBH2QcKVEO" }
}


def verify_path(path, method):
    pass


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


## handle datetime fields and convert them to epoch
class FormatAsJson(beam.DoFn):
    def process(self, element):
        # Convert the element to a JSON string
        import json

        for k, v in element.items():
            if isinstance(v, datetime.datetime):
                element[k] = v.timestamp()
            elif isinstance(v, datetime.date):
                element[k] = v.isoformat()
        yield json.dumps(element)


def run_mongodb_pipeline(path, transformations):
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        lines = pipeline | "ReadMyDatabase" >> beam.io.ReadFromMongoDB(
            uri=path["url"], db=path["database"], coll=path["collection"]
        )
        lines = lines | "RemoveObjectId" >> beam.Map(
            lambda line: {k: v for k, v in line.items() if k != "_id" and k != "__v"}
        )
        for transformation in transformations:
            # if transformation['include_fields'] is not None:
            #     lines = lines | 'IncludeFields' >> beam.Map(lambda line: {k: v for k, v in line.items() if k in transformation['include_fields']})
            if transformation["remove_null"] is not None:
                ## dont include the documents which have null values for specific fields mentioned in transformation['remove_null']['fields']
                lines = lines | "RemoveNull" >> beam.Filter(
                    lambda line: all([line.get(field) is not None for field in transformation["remove_null"]["fields"]])
                )
            # if transformation['group_by'] is not None:
            #     lines = lines | 'GroupBy' >> beam.GroupBy(lambda line: line[transformation['group_by']['field']])
            # if transformation['sort'] is not None:
            #     lines = lines | 'Sort' >> beam.Map(lambda line: sorted(line, key=lambda x: x[transformation['sort']['field']]))
            # remove ObjectId from the output
            # lines | "WriteToCsv" >> beam.io.WriteToText(
            #     "output.csv", file_name_suffix=".csv"
            # )
            lines | "WriteToJSON" >> beam.ParDo(FormatAsJson()) | beam.io.WriteToText(
                "output.json", file_name_suffix=".json"
            )
            # for sink in transformation['sinks']:


def run_file_pipeline(path, transformations):
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        lines = pipeline | "ReadMyFile" >> beam.io.ReadFromText(path["path"], skip_header_lines=1)
        for transformation in transformations:
            if transformation["remove_null"] is not None:
                ## remove nulls for specific fields mentioned in transformation['remove_null']['fields'] using mean in case of numeric fields and mode in case of categorical fields
                lines = lines | "RemoveNull" >> beam.Map(lambda line: {k: v for k, v in line.items() if v is not None})
            # if transformation['group_by'] is not None:
            #     lines = lines | 'GroupBy' >> beam.GroupBy(lambda line: line[transformation['group_by']['field']])
            # if transformation['sort'] is not None:
            #     lines = lines | 'Sort' >> beam.Map(lambda line: sorted(line, key=lambda x: x[transformation['sort']['field']]))
            lines | "WriteToJson" >> beam.ParDo(FormatAsJson()) | beam.io.WriteToText(
                "output.json", file_name_suffix=".json"
            )


def run_stream_pipeline(sources):
    for source in sources:
        if source["method"] == "kafka":
            run_kafka_pipeline(source["path"], source["transformations"])


def run_batch_pipeline(sources):
    for source in sources:
        if source["method"] == "mongodb":
            run_mongodb_pipeline(source["path"], source["transformations"])
        if source["method"] == "file":
            run_file_pipeline(source["path"], source["transformations"])


def run_kafka_pipeline(path, transformations):
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        lines = lines = pipeline | "ReadMyKafka" >> beam.io.Read(
            consumer_config=kafka_config,
            topics=[path["topic"]],
            timestamp_attribute="timestamp",
        )
        for transformation in transformations:
            # if transformation['include_fields'] is not None:
            #     lines = lines | 'IncludeFields' >> beam.Map(lambda line: {k: v for k, v in line.items() if k in transformation['include_fields']})
            if transformation["remove_null"] is not None:
                ## remove nulls for specific fields mentioned in transformation['remove_null']['fields'] using mean in case of numeric fields and mode in case of categorical fields
                lines = lines | "RemoveNull" >> beam.Map(lambda line: {k: v for k, v in line.items() if v is not None})
            # if transformation['group_by'] is not None:
            #     lines = lines | 'GroupBy' >> beam.GroupBy(lambda line: line[transformation['group_by']['field']])
            # if transformation['sort'] is not None:
            #     lines = lines | 'Sort' >> beam.Map(lambda line: sorted(line, key=lambda x: x[transformation['sort']['field']]))
            lines | "WriteToCsv" >> beam.io.WriteToText("output.csv", file_name_suffix=".csv")


def run_pipeline(config):
    stream_sources = []
    batch_sources = []
    for source in config["sources"]:
        if source["type"] == "stream":
            stream_sources.append(source)
        elif source["type"] == "batch":
            batch_sources.append(source)
        else:
            raise ValueError("Invalid source type")
    if len(batch_sources) > 0:
        run_batch_pipeline(batch_sources)
    # if len(stream_sources) > 0:
    #     run_stream_pipeline(stream_sources)


def csv_to_json():
    with beam.Pipeline() as pipeline:
        lines = pipeline | "ReadMyFile" >> beam.io.ReadFromText("input.csv", skip_header_lines=1)

        parsed_data = lines | "ParseCSV" >> beam.Map(lambda line: next(csv.reader([line])))

        def to_json(row):
            keys = ["name", "UID", "address"]
            return json.dumps(dict(zip(keys, row)))

        json_data = parsed_data | "ConvertToJSON" >> beam.Map(to_json)

        # Write to the JSON file
        json_data | "WriteJSON" >> beam.io.WriteToText("output.json", file_name_suffix=".json")
