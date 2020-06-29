"""pipeline.py"""
import json
import logging
import warnings
from datetime import datetime
from typing import List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pydantic import BaseModel  # pylint: disable=no-name-in-module


class Product(BaseModel):
    id: str
    quantity: str


class Message(BaseModel):
    user: str
    products: List[Product]
    timestamp: datetime


class ParseMessage(beam.DoFn):
    def process(self, json_string, timestamp=beam.DoFn.TimestampParam):
        message_dict = json.loads(json_string)
        message = Message(**message_dict, timestamp=timestamp.to_utc_datetime())

        yield message


class Log(beam.DoFn):
    def process(self, element):
        logging.info("")
        logging.info("ELEMENT:")
        logging.info(str(element))

        yield element


class StreamOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_topic",
            help="The Cloud Pub/Sub topic to read from.\n"
            '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
        )


def flat_function(key, elements):
    for element in elements:
        yield (key, element)


def execute_pipeline(pipeline, options):
    messages = (
        pipeline
        | "read messages" >> beam.io.ReadFromPubSub(topic=options.input_topic)
        | "parse to messages" >> beam.ParDo(ParseMessage())
    )
    sessions = (
        messages
        | "window" >> beam.WindowInto(beam.window.Sessions(15))
        | "add key" >> beam.Map(lambda element: (element.user, element.products))
        | "group by user" >> beam.GroupByKey()
        | "first flatten" >> beam.FlatMapTuple(flat_function)
        | "second flatten" >> beam.FlatMapTuple(flat_function)
    ) | "log 1" >> beam.ParDo(Log())

    products = (
        sessions
        | "add new key" >> beam.Map(lambda session: (session[1].id, session[1]))
        | "group by product" >> beam.GroupByKey()
    ) | "log 2" >> beam.ParDo(Log())


def run():

    pipeline_options = PipelineOptions(streaming=True)
    stream_options = pipeline_options.view_as(StreamOptions)

    runner_name = stream_options.get_all_options().get("runner")

    if runner_name == "DirectRunner":
        with beam.Pipeline(options=stream_options) as pipeline:
            execute_pipeline(pipeline, stream_options)

    elif runner_name == "DataflowRunner":
        pipeline = beam.Pipeline(options=stream_options)
        execute_pipeline(pipeline, stream_options)
        pipeline.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )
    warnings.filterwarnings(
        "ignore", "google.auth._default",
    )
    run()
