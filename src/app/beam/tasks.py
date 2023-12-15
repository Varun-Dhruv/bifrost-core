from __future__ import absolute_import, unicode_literals
from celery import shared_task
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam.transformations import transform
from beam.loaders import load
from beam.extractors import ExtractorFactory


@shared_task
def run_batch_job(job):
    extractor = ExtractorFactory(job["method"], job["type"])
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        lines = extractor(pipeline, job["path"])
        lines = transform(lines, pipeline, job["transforms"])
        load(lines, pipeline, job["sink"])
    return True


@shared_task
def run_stream_job(src, transformation, sink):
    print("Running stream job")
