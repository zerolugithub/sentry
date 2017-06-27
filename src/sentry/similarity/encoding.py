from __future__ import absolute_import

from collections import Mapping, Sequence

import six

from sentry.interfaces.stacktrace import Frame


def get_frame_signature(frame, lines=5):
    assert frame.context_line is not None
    return u'\n'.join(
        (frame.pre_context or [])[-lines:] +
        [frame.context_line] +
        (frame.post_context or [])[:lines]
    )


def get_frame_data(frame):
    attributes = {}

    if frame.function in set(['<lambda>', None]):
        attributes['signature'] = get_frame_signature(frame)
    else:
        attributes['function'] = frame.function

    for attribute in ['module', 'filename']:
        value = getattr(frame, attribute, None)
        if value is not None:
            attributes[attribute] = value
            break

    return attributes


def encode(value):
    if isinstance(value, Frame):
        value = get_frame_data(value)

    if isinstance(value, six.text_type):
        return value.encode('utf8')
    elif isinstance(value, six.binary_type):
        return value
    elif isinstance(value, Sequence):
        return '\x00'.join(map(encode, value))
    elif isinstance(value, Mapping):
        return '\x00'.join(sorted('\x01'.join(map(encode, item)) for item in value.items()))
    else:
        raise TypeError('Unsupported type: {}'.format(type(value)))
