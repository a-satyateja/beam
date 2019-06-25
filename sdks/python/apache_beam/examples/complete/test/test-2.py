#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

from __future__ import absolute_import
import argparse
import logging
import os

import apache_beam as beam
from PIL import Image
from apache_beam.io.gcp import gcsio
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.io.filesystem import MatchResult
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from io import BytesIO
from apache_beam.io import filesystems


class WordExtractingDoFn(beam.DoFn):

    def process(self, element):
        print("reads element ::")
        print(element)
        head, file_name = os.path.split(os.path.splitext(element)[0])
        gcsio_obj = gcsio.GcsIO()
        bufferImg = gcsio_obj.open(element, 'r').read()
        image = Image.open(BytesIO(bufferImg))
        writer = filesystems.FileSystems.create(head + "/" + file_name + ".png", "image/png")
        b = BytesIO()
        image.save(b, format="png")
        contents = b.getvalue()
        writer.write(contents)
        writer.close()
        gcsio_obj.delete(element)
        b.close()


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # files = (p | 'files' >> filesystems.FileSystems.match(known_args.input))
    # print (files)
    # files_and_contents = (files | 'read' >> beam.Map(lambda x: x.metadata.path))
    # print (files_and_contents)
    # counts = (files_and_contents | 'read-1' >> (beam.ParDo(WordExtractingDoFn())))

    files = filesystems.FileSystems.match(known_args.input)
    print (files)
    print (len(files))
    for afile in files:  # by item
        print(FileMetadata(afile))


    # Read the text file[pattern] into a PCollection.
    lines = p | 'read' >> ReadFromText(known_args.input)

    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    counts = (lines
              | 'split' >> (beam.ParDo(WordExtractingDoFn())
                            .with_output_types(unicode))
              | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(count_ones))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
