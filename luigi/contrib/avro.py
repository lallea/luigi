import logging

from luigi.format import Format

logger = logging.getLogger('luigi-interface')

try:
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter
    from avro.schema import Schema, SchemaFromJSONData
except ImportError:
    logger.warning('avro module imported, but avro is not installed. '
                   'Will fail at runtime if Avro functionality is used.')


class AvroReader:
    def __init__(self, stream):
        self._reader = DataFileReader(stream, DatumReader())

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def close(self):
        self._reader.close()

    def read(self, n=-1):
        if n == -1:
            return list(self)
        else:
            return list(itertools.islice(iter(self), n))

    def __iter__(self):
        for record in self._reader:
            yield record

    def __next__(self):
        return next(self)


class AvroWriter:
    def __init__(self, stream, schema):
        self._stream = stream
        self._schema = schema
        self._writer = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def close(self):
        if self._writer is None:
            self._stream.close()
        else:
            self._writer.close()

    def _guess_schema(self, record):
        fields = [self._guess_field(k, v) for k, v in record.items()]
        return SchemaFromJSONData({
            'type': 'record',
            'name': 'UnknownType',
            'fields': fields
        })

    def _guess_field(self, key, value):
        return {'name': key, 'type': self._guess_type(value)}

    def _guess_type(self, value):
        return {str: 'string'}.get(type(value), type(value).__name__)

    def write(self, record):
        if self._writer is None:
            self._writer = DataFileWriter(self._stream, DatumWriter(), self._schema or self._guess_schema(record))
        self._writer.append(record)


class AvroFormat(Format):
    input = 'dict'
    output = 'avro'

    def __init__(self, schema=None):
        self._schema = schema

    def pipe_reader(self, input_pipe):
        return AvroReader(input_pipe)

    def pipe_writer(self, output_pipe):
        return AvroWriter(output_pipe, self._schema)
