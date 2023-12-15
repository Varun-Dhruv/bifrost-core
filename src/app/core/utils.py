from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam

def test_run():
    options = PipelineOptions(
        runner="PortableRunner",
        job_endpoint="job-server:8099",
        artifact_endpoint="job-server:8098",
        flink_version="1.16",
        environment_type="EXTERNAL",
        environment_config="flink-taskmanager:50000",
    )
    with apache_beam.Pipeline(options=options) as p:
        (
            p
            | "Create words" >> apache_beam.Create(["to be or not to be"])
            | "Split words" >> apache_beam.FlatMap(lambda words: words.split(" "))
            | "Write to file" >> apache_beam.io.WriteToText("test.txt")
        )
        result=p.run().wait_until_finish()
        print(result)
        