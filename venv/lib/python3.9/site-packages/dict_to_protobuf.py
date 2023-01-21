#coding=utf-8
import logging
import six
from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor

l = logging.getLogger('dict_to_protbuf')

__all__ = ['dict_to_protobuf', "protobuf_to_dict"]


# ---------------------------------------------------------------------
# --  Change DICT to Protobuf
# ---------------------------------------------------------------------
def parse_list(values,message):
    '''parse list to protobuf message'''
    if values and isinstance(values[0], dict):#value needs to be further parsed
        for v in values:
            cmd = message.add()
            parse_dict(v,cmd)
    else:#value can be set
        message.extend(values)


def parse_dict(values,message):
    if six.PY2:
        iterator = values.iteritems()
    elif six.PY3:
        iterator = values.items()
    for k,v in iterator:
        try:
            if isinstance(v, dict):#value needs to be further parsed
                parse_dict(v,getattr(message,k))
            elif isinstance(v, list):
                parse_list(v,getattr(message,k))
            else:#value can be set
                setattr(message, k, v)
        except AttributeError:
            logging.basicConfig()
            l.warning('try to access invalid attributes %r.%r = %r', message, k, v)

def dict_to_protobuf(value,message):
    if isinstance(value, dict):
        parse_dict(value, message)
    elif hasattr(value, "DESCRIPTOR"):
        parse_dict(message, value)
    elif isinstance(value, list):
        # assume the first key is a key to a list obj
        guessed_key = ""
        try:
            guessed_key = message.DESCRIPTOR.fields_by_name.items()[0][0]  # may raise IndexError
        except TypeError:
            guessed_key = list(message.DESCRIPTOR.fields_by_name.keys())[0]
        parse_dict({guessed_key: value}, message)


# ---------------------------------------------------------------------
# --  Change Protobuf to DICT (from package protobuf_to_dict)
# ---------------------------------------------------------------------

EXTENSION_CONTAINER = '___X'

if six.PY2:
    TYPE_CALLABLE_MAP = {
        FieldDescriptor.TYPE_DOUBLE: float,
        FieldDescriptor.TYPE_FLOAT: float,
        FieldDescriptor.TYPE_INT32: int,
        FieldDescriptor.TYPE_INT64: long,
        FieldDescriptor.TYPE_UINT32: int,
        FieldDescriptor.TYPE_UINT64: long,
        FieldDescriptor.TYPE_SINT32: int,
        FieldDescriptor.TYPE_SINT64: long,
        FieldDescriptor.TYPE_FIXED32: int,
        FieldDescriptor.TYPE_FIXED64: long,
        FieldDescriptor.TYPE_SFIXED32: int,
        FieldDescriptor.TYPE_SFIXED64: long,
        FieldDescriptor.TYPE_BOOL: bool,
        FieldDescriptor.TYPE_STRING: unicode,
        FieldDescriptor.TYPE_BYTES: lambda b: b.encode("base64"),
        FieldDescriptor.TYPE_ENUM: int,
    }
else:

    TYPE_CALLABLE_MAP = {
        FieldDescriptor.TYPE_DOUBLE: float,
        FieldDescriptor.TYPE_FLOAT: float,
        FieldDescriptor.TYPE_INT32: int,
        FieldDescriptor.TYPE_INT64: int,
        FieldDescriptor.TYPE_UINT32: int,
        FieldDescriptor.TYPE_UINT64: int,
        FieldDescriptor.TYPE_SINT32: int,
        FieldDescriptor.TYPE_SINT64: int,
        FieldDescriptor.TYPE_FIXED32: int,
        FieldDescriptor.TYPE_FIXED64: int,
        FieldDescriptor.TYPE_SFIXED32: int,
        FieldDescriptor.TYPE_SFIXED64: int,
        FieldDescriptor.TYPE_BOOL: bool,
        FieldDescriptor.TYPE_STRING: str,
        FieldDescriptor.TYPE_BYTES: bytes,
        FieldDescriptor.TYPE_ENUM: int,
    }

def repeated(type_callable):
    return lambda value_list: [type_callable(value) for value in value_list]
def enum_label_name(field, value):
    return field.enum_type.values_by_number[int(value)].name

def protobuf_to_dict(pb, type_callable_map=TYPE_CALLABLE_MAP, use_enum_labels=False):
    result_dict = {}
    extensions = {}
    for field, value in pb.ListFields():
        type_callable = _get_field_value_adaptor(pb, field, type_callable_map, use_enum_labels)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            type_callable = repeated(type_callable)

        if field.is_extension:
            extensions[str(field.number)] = type_callable(value)
            continue

        result_dict[field.name] = type_callable(value)

    if extensions:
        result_dict[EXTENSION_CONTAINER] = extensions
    return result_dict


def _get_field_value_adaptor(pb, field, type_callable_map=TYPE_CALLABLE_MAP, use_enum_labels=False):
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        # recursively encode protobuf sub-message
        return lambda pb: protobuf_to_dict(pb, type_callable_map=type_callable_map, use_enum_labels=use_enum_labels)

    if use_enum_labels and field.type == FieldDescriptor.TYPE_ENUM:
        return lambda value: enum_label_name(field, value)

    if field.type in type_callable_map:
        return type_callable_map[field.type]

    raise TypeError("Field %s.%s has unrecognised type id %d" % (pb.__class__.__name__, field.name, field.type))