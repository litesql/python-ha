"""Converter utilities for protobuf Any type conversion."""

from datetime import datetime
from typing import Any, Optional

from google.protobuf.any_pb2 import Any as AnyProto
from google.protobuf.empty_pb2 import Empty
from google.protobuf.wrappers_pb2 import (
    BoolValue,
    BytesValue,
    DoubleValue,
    FloatValue,
    Int32Value,
    Int64Value,
    StringValue,
    UInt32Value,
    UInt64Value,
)
from google.protobuf.timestamp_pb2 import Timestamp

TYPE_URL_PREFIX = "type.googleapis.com/google.protobuf."


def to_any(value: Any) -> AnyProto:
    """Convert a Python value to protobuf Any format."""
    any_proto = AnyProto()

    if value is None:
        any_proto.Pack(Empty())
        return any_proto

    if isinstance(value, str):
        any_proto.Pack(StringValue(value=value))
        return any_proto

    if isinstance(value, bool):
        any_proto.Pack(BoolValue(value=value))
        return any_proto

    if isinstance(value, int):
        if -2147483648 <= value <= 2147483647:
            any_proto.Pack(Int32Value(value=value))
        else:
            any_proto.Pack(Int64Value(value=value))
        return any_proto

    if isinstance(value, float):
        any_proto.Pack(DoubleValue(value=value))
        return any_proto

    if isinstance(value, datetime):
        ts = Timestamp()
        ts.FromDatetime(value)
        any_proto.Pack(ts)
        return any_proto

    if isinstance(value, bytes):
        any_proto.Pack(BytesValue(value=value))
        return any_proto

    if isinstance(value, bytearray):
        any_proto.Pack(BytesValue(value=bytes(value)))
        return any_proto

    raise TypeError(f"Unsupported type: {type(value)}")


def from_any(any_proto: AnyProto) -> Optional[Any]:
    """Convert a protobuf Any value to Python value."""
    if not any_proto or not any_proto.type_url:
        return None

    type_url = any_proto.type_url

    if type_url == f"{TYPE_URL_PREFIX}Empty":
        return None

    if type_url == f"{TYPE_URL_PREFIX}StringValue":
        wrapper = StringValue()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}DoubleValue":
        wrapper = DoubleValue()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}FloatValue":
        wrapper = FloatValue()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}Int64Value":
        wrapper = Int64Value()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}Int32Value":
        wrapper = Int32Value()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}UInt64Value":
        wrapper = UInt64Value()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}UInt32Value":
        wrapper = UInt32Value()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}BoolValue":
        wrapper = BoolValue()
        any_proto.Unpack(wrapper)
        return wrapper.value

    if type_url == f"{TYPE_URL_PREFIX}Timestamp":
        ts = Timestamp()
        any_proto.Unpack(ts)
        return ts.ToDatetime()

    if type_url == f"{TYPE_URL_PREFIX}BytesValue":
        wrapper = BytesValue()
        any_proto.Unpack(wrapper)
        return wrapper.value

    raise ValueError(f"Unsupported type: {type_url}")
