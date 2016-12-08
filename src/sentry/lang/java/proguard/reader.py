from __future__ import absolute_import

import itertools
import operator
from collections import namedtuple


Class = namedtuple('Class', [
    'old',
    'new',
])

Field = namedtuple('Field', [
    'type',
    'old',
    'new',
])

Method = namedtuple('Method', [
    'first_line',
    'last_line',
    'old',
    'arguments',
    'new'
])


def lookahead(iterator):
    actual, ahead = itertools.tee(iterator)
    next(ahead, None)
    for value in actual:
        yield value, next(ahead, None)


arrow_separator = '->'


def process_class_mapping(line):
    arrow_index = line.find(arrow_separator)
    if arrow_index == -1:
        return

    colon_index = line.find(':', arrow_index + 2)
    if colon_index == -1:
        return

    return Class(*map(
        operator.methodcaller('strip'),
        line[:colon_index].split(arrow_separator),
    ))


def process_class_member_mapping(line):
    colon_index_1 = line.find(':')
    colon_index_2 = line.find(':', colon_index_1 + 1) if colon_index_1 else -1

    # NOTE: This seems a little strange, but it's a direct port of the retrace logic.
    space_index = line.find(' ', colon_index_2 + 2)

    argument_index_1 = line.find('(', space_index + 1)
    argument_index_2 = line.find(')', argument_index_1 + 1) if argument_index_1 else -1

    arrow_index = line.find('->', max(space_index, argument_index_2 + 1))

    if not space_index or not arrow_index:
        return

    type = line[(colon_index_2 + 1):space_index].strip()
    name = line[(space_index + 1):(argument_index_1 if argument_index_1 >= 0 else arrow_index)].strip()
    new_name = line[arrow_index + 2:].strip()

    if not type or not name or not new_name:
        return

    if argument_index_2 < 0:
        return Field(type, name, new_name)
    else:
        first_line_number = None
        last_line_number = None
        if colon_index_2 > 0:
            first_line_number = int(line[:colon_index_1].strip())
            last_line_number = int(line[(colon_index_1 + 1):colon_index_2].strip())
        arguments = line[(argument_index_1 + 1):argument_index_2].strip()
        return Method(first_line_number, last_line_number, name, arguments, new_name)


line_processors = [
    process_class_mapping,
    process_class_member_mapping,
]


def process_line(line):
    line = line.strip()
    for processor in line_processors:
        # TODO: Return errors from these, collect them and return them as part
        # of the overarching `ValueError`.
        result = processor(line)
        if result is not None:
            return result
    raise ValueError


def read(file):
    group = None
    for current, upcoming in lookahead(itertools.imap(process_line, file)):
        if group is None:
            assert isinstance(current, Class)

        if isinstance(current, Class):
            group = current, []
        else:
            group[1].append(current)

        if isinstance(upcoming, Class):
            yield group

    yield group


def index(stream):
    idx = {}
    for cls, members in stream:
        assert cls.new not in idx
        member_idx = {m.new: m.old for m in members if m.old != m.new}
        if cls.new != cls.old or member_idx:
            idx[cls.new] = (cls.old, member_idx)
    return idx
