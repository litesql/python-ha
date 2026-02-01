"""Generated gRPC stubs - to be generated from proto files."""

import grpc
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


class QueryRequest:
    """Query request message."""
    pass


class QueryResponse:
    """Query response message."""
    pass


class DownloadRequest:
    """Download request message."""
    pass


class DownloadResponse:
    """Download response message."""
    pass


class ReplicationIDsResponse:
    """Replication IDs response message."""
    pass


class DatabaseServiceStub:
    """
    gRPC stub for DatabaseService.

    This is a placeholder that should be replaced with generated stubs from:
        python -m grpc_tools.protoc -I./proto --python_out=./src/litesql_ha/_generated \
            --grpc_python_out=./src/litesql_ha/_generated ./proto/sql.proto
    """

    def __init__(self, channel: grpc.aio.Channel):
        """Initialize the stub."""
        self.Query = channel.stream_stream(
            "/sql.v1.DatabaseService/Query",
            request_serializer=self._serialize_query_request,
            response_deserializer=self._deserialize_query_response,
        )
        self.Download = channel.unary_stream(
            "/sql.v1.DatabaseService/Download",
            request_serializer=self._serialize_download_request,
            response_deserializer=self._deserialize_download_response,
        )
        self.LatestSnapshot = channel.unary_stream(
            "/sql.v1.DatabaseService/LatestSnapshot",
            request_serializer=self._serialize_download_request,
            response_deserializer=self._deserialize_download_response,
        )
        self.ReplicationIDs = channel.unary_unary(
            "/sql.v1.DatabaseService/ReplicationIDs",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=self._deserialize_replication_ids_response,
        )

    def _serialize_query_request(self, request: dict) -> bytes:
        """Serialize a query request to protobuf bytes."""
        from google.protobuf import descriptor_pool, message_factory
        import struct

        result = b""

        if request.get("replication_id"):
            value = request["replication_id"].encode("utf-8")
            result += b"\x0a" + self._encode_varint(len(value)) + value

        if request.get("sql"):
            value = request["sql"].encode("utf-8")
            result += b"\x12" + self._encode_varint(len(value)) + value

        if request.get("type"):
            result += b"\x18" + self._encode_varint(request["type"])

        if request.get("params"):
            for param in request["params"]:
                param_bytes = self._serialize_named_value(param)
                result += b"\x22" + self._encode_varint(len(param_bytes)) + param_bytes

        return result

    def _serialize_named_value(self, param: dict) -> bytes:
        """Serialize a NamedValue to protobuf bytes."""
        result = b""

        if param.get("name"):
            value = param["name"].encode("utf-8")
            result += b"\x0a" + self._encode_varint(len(value)) + value

        if param.get("ordinal"):
            result += b"\x10" + self._encode_varint(param["ordinal"])

        if param.get("value"):
            any_bytes = param["value"].SerializeToString()
            result += b"\x1a" + self._encode_varint(len(any_bytes)) + any_bytes

        return result

    def _encode_varint(self, value: int) -> bytes:
        """Encode an integer as a varint."""
        result = []
        while value > 0x7F:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result) if result else b"\x00"

    def _deserialize_query_response(self, data: bytes) -> "QueryResponseWrapper":
        """Deserialize a query response from protobuf bytes."""
        return QueryResponseWrapper(data)

    def _serialize_download_request(self, request: dict) -> bytes:
        """Serialize a download request."""
        result = b""
        if request.get("replication_id"):
            value = request["replication_id"].encode("utf-8")
            result += b"\x0a" + self._encode_varint(len(value)) + value
        return result

    def _deserialize_download_response(self, data: bytes) -> "DownloadResponseWrapper":
        """Deserialize a download response."""
        return DownloadResponseWrapper(data)

    def _deserialize_replication_ids_response(self, data: bytes) -> "ReplicationIDsResponseWrapper":
        """Deserialize a replication IDs response."""
        return ReplicationIDsResponseWrapper(data)


class ResultSetWrapper:
    """Wrapper for ResultSet message."""

    def __init__(self):
        self.columns = []
        self.rows = []


class RowWrapper:
    """Wrapper for Row message."""

    def __init__(self):
        self.values = []


class QueryResponseWrapper:
    """Wrapper for QueryResponse message."""

    def __init__(self, data: bytes):
        self.result_set = ResultSetWrapper()
        self.rows_affected = 0
        self.txseq = 0
        self.error = ""
        self._parse(data)

    def _parse(self, data: bytes):
        """Parse the protobuf data."""
        offset = 0
        while offset < len(data):
            if offset >= len(data):
                break

            tag_byte = data[offset]
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1

            if field_num == 1 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                self._parse_result_set(data[offset : offset + length])
                offset += length
            elif field_num == 2 and wire_type == 0:
                self.rows_affected, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
            elif field_num == 3 and wire_type == 0:
                self.txseq, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
            elif field_num == 4 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                self.error = data[offset : offset + length].decode("utf-8")
                offset += length
            else:
                break

    def _parse_result_set(self, data: bytes):
        """Parse a ResultSet message."""
        offset = 0
        while offset < len(data):
            if offset >= len(data):
                break

            tag_byte = data[offset]
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1

            if field_num == 1 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                self.result_set.columns.append(data[offset : offset + length].decode("utf-8"))
                offset += length
            elif field_num == 2 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                row = self._parse_row(data[offset : offset + length])
                self.result_set.rows.append(row)
                offset += length
            else:
                break

    def _parse_row(self, data: bytes) -> RowWrapper:
        """Parse a Row message."""
        from google.protobuf.any_pb2 import Any as AnyProto

        row = RowWrapper()
        offset = 0

        while offset < len(data):
            if offset >= len(data):
                break

            tag_byte = data[offset]
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1

            if field_num == 1 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                any_proto = AnyProto()
                any_proto.ParseFromString(data[offset : offset + length])
                row.values.append(any_proto)
                offset += length
            else:
                break

        return row

    def _decode_varint(self, data: bytes, offset: int) -> tuple:
        """Decode a varint from data."""
        result = 0
        shift = 0
        bytes_read = 0

        while offset + bytes_read < len(data):
            byte = data[offset + bytes_read]
            result |= (byte & 0x7F) << shift
            bytes_read += 1
            if (byte & 0x80) == 0:
                break
            shift += 7

        return result, bytes_read


class DownloadResponseWrapper:
    """Wrapper for DownloadResponse message."""

    def __init__(self, data: bytes):
        self.data = b""
        self._parse(data)

    def _parse(self, data: bytes):
        """Parse the protobuf data."""
        offset = 0
        while offset < len(data):
            if offset >= len(data):
                break

            tag_byte = data[offset]
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1

            if field_num == 1 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                self.data = data[offset : offset + length]
                offset += length
            else:
                break

    def _decode_varint(self, data: bytes, offset: int) -> tuple:
        """Decode a varint from data."""
        result = 0
        shift = 0
        bytes_read = 0

        while offset + bytes_read < len(data):
            byte = data[offset + bytes_read]
            result |= (byte & 0x7F) << shift
            bytes_read += 1
            if (byte & 0x80) == 0:
                break
            shift += 7

        return result, bytes_read


class ReplicationIDsResponseWrapper:
    """Wrapper for ReplicationIDsResponse message."""

    def __init__(self, data: bytes):
        self.replication_id = []
        self._parse(data)

    def _parse(self, data: bytes):
        """Parse the protobuf data."""
        offset = 0
        while offset < len(data):
            if offset >= len(data):
                break

            tag_byte = data[offset]
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1

            if field_num == 1 and wire_type == 2:
                length, bytes_read = self._decode_varint(data, offset)
                offset += bytes_read
                self.replication_id.append(data[offset : offset + length].decode("utf-8"))
                offset += length
            else:
                break

    def _decode_varint(self, data: bytes, offset: int) -> tuple:
        """Decode a varint from data."""
        result = 0
        shift = 0
        bytes_read = 0

        while offset + bytes_read < len(data):
            byte = data[offset + bytes_read]
            result |= (byte & 0x7F) << shift
            bytes_read += 1
            if (byte & 0x80) == 0:
                break
            shift += 7

        return result, bytes_read
