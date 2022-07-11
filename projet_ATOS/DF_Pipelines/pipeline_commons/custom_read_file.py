import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io import range_trackers
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.util import Reshuffle
from apache_beam.coders import coders
from apache_beam.io.textio import _create_text_source
from functools import partial
#from IPython import embed


DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024


def _determine_splittability_from_compression_type(
    file_path, compression_type):
  if compression_type == CompressionTypes.AUTO:
    compression_type = CompressionTypes.detect_compression_type(file_path)

  return compression_type == CompressionTypes.UNCOMPRESSED


class custom_ReadRange(beam.io.filebasedsource._ReadRange):
    def process(self, element, *args, **kwargs):
        metadata, range = element["data"]
        source = self._source_from_file(metadata.path)
        #embed()
        # Following split() operation has to be performed to create a proper
        # _SingleFileSource. Otherwise what we have is a ConcatSource that contains
        # a single _SingleFileSource. ConcatSource.read() expects a RangeTraker for
        # sub-source range and reads full sub-sources (not byte ranges).
        source = list(source.split(float('inf')))[0].source
        #embed()
        for record in source.read(range.new_tracker()):
            yield {"data":record, "message":element["message"]}


class custom_ExpandIntoRanges(beam.io.filebasedsource._ExpandIntoRanges):
    def process(self, element, *args, **kwargs):
        fileURL=element["fileURL"]
        match_results = FileSystems.match([fileURL])
        for metadata in match_results[0].metadata_list:
            splittable = (
                self._splittable and
                _determine_splittability_from_compression_type(
                    metadata.path, self._compression_type))

        if splittable:
            for split in OffsetRange(
                0, metadata.size_in_bytes).split(
                    self._desired_bundle_size, self._min_bundle_size):
                yield {"data":(metadata, split),"message":element}
        else:
            yield {"data":(metadata, OffsetRange(
                0, range_trackers.OffsetRangeTracker.OFFSET_INFINITY)),"message":element}

class custom_ReadAllFiles(beam.io.filebasedsource.ReadAllFiles):
    def expand(self, pvalue):
        return (pvalue
                | 'ExpandIntoRanges' >> ParDo(custom_ExpandIntoRanges(
                    self._splittable, self._compression_type,
                    self._desired_bundle_size, self._min_bundle_size))
                | 'Reshard' >> Reshuffle()
                | 'ReadRange' >> ParDo(custom_ReadRange(self._source_from_file)))


class custom_ReadAllFromText(beam.io.ReadAllFromText):
    def __init__(
      self,
      min_bundle_size=0,
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
      compression_type=CompressionTypes.AUTO,
      strip_trailing_newlines=True,
      coder=coders.StrUtf8Coder(),  # type: coders.Coder
      skip_header_lines=0,
      with_filename=False,
      delimiter=None,
      escapechar=None,
      **kwargs):
        super().__init__(**kwargs)
        source_from_file = partial(
            _create_text_source,
            min_bundle_size=min_bundle_size,
            compression_type=compression_type,
            strip_trailing_newlines=strip_trailing_newlines,
            coder=coder,
            skip_header_lines=skip_header_lines,
            delimiter=delimiter,
            escapechar=escapechar)
        self._desired_bundle_size = desired_bundle_size
        self._min_bundle_size = min_bundle_size
        self._compression_type = compression_type
        self._read_all_files = custom_ReadAllFiles(
            True,
            compression_type,
            desired_bundle_size,
            min_bundle_size,
            source_from_file,
            with_filename)