from apache_beam.options.pipeline_options import PipelineOptions

API_VERSIONS = ["v1", "v2"]
SOURCE_TYPE = ["stream", "batch"]
KIND = ["execute", "schedule"]
BATCH_METHODS = ["mongodb", "file"]
STREAM_METHODS = ["kafka"]

pipeline_options = PipelineOptions(
    runner="PortableRunner",
    job_endpoint="job-server:8099",
    artifact_endpoint="job-server:8098",
    flink_version="1.16",
    environment_type="EXTERNAL",
    environment_config="flink-taskmanager:50000",
)
