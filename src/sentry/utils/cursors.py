"""
sentry.utils.cursors
~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010-2014 by the Sentry Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import

import six

from collections import Sequence


class Cursor(object):
    def __init__(self, value, offset=0, is_prev=False, has_results=None):
        # XXX: ceil is not entirely correct here, but it's a simple hack
        # that solves most problems
        self.value = int(value)
        self.offset = int(offset)
        self.is_prev = bool(is_prev)
        self.has_results = has_results

    def __str__(self):
        return '%s:%s:%s' % (self.value, self.offset, int(self.is_prev))

    def __repr__(self):
        return '<%s: value=%s offset=%s is_prev=%s>' % (
            type(self), self.value, self.offset, int(self.is_prev)
        )

    def __nonzero__(self):
        return self.has_results

    @classmethod
    def from_string(cls, value):
        bits = value.split(':')
        if len(bits) != 3:
            raise ValueError
        try:
            bits = float(bits[0]), int(bits[1]), int(bits[2])
        except (TypeError, ValueError):
            raise ValueError
        return cls(*bits)


class CursorResult(Sequence):
    def __init__(self, results, next, prev, hits=None, max_hits=None):
        self.results = results
        self.next = next
        self.prev = prev
        self.hits = hits
        self.max_hits = max_hits

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, key):
        return self.results[key]

    def __repr__(self):
        return '<%s: results=%s>' % (type(self).__name__, len(self.results))

    @classmethod
    def from_ids(cls, id_list, key=None, limit=100, cursor=None):
        from sentry.models import Group

        group_map = Group.objects.in_bulk(id_list)

        results = []
        for g_id in id_list:
            try:
                results.append(group_map[g_id])
            except KeyError:
                pass

        return build_cursor(
            results=results,
            key=key,
            cursor=cursor,
            limit=limit,
        )


def build_cursor(results, key, limit=100, is_desc=False, cursor=None, hits=None, max_hits=None):
    if cursor is None:
        cursor = Cursor(0, 0, 0)

    value = cursor.value
    offset = cursor.offset
    is_prev = cursor.is_prev

    num_results = len(results)

    if is_prev:
        has_prev = num_results > limit
        num_results = len(results)
    elif value or offset:
        # It's likely that there's a previous page if they passed us either offset values
        has_prev = True
    else:
        # we don't know
        has_prev = False

    # Default cursor if not present
    if is_prev:
        next_value = value
        next_offset = 0
        has_next = True
    elif num_results:
        if not value:
            value = int(key(results[0]))

        # Are there more results than whats on the current page?
        has_next = num_results > limit

        # Determine what our next cursor is by ensuring we have a unique offset
        next_value = int(key(results[-1]))

        if next_value == value:
            # value has not changed, page forward by adjusting the offset
            next_offset = offset + limit
        else:
            # We have an absolute value to page from. If any of the items in
            # the current result set come *after* or *before* (depending on the
            # is_desc flag) we will want to increment the offset to account for
            # moving past them.
            #
            # This is required to account for loss of precision in the key value.
            next_offset = 0
            result_iter = reversed(results)

            # If we have more results the last item in the results should be
            # skipped, as we know we want to start from that item and do not
            # need to offset from it.
            if has_next:
                six.next(result_iter)

            for result in result_iter:
                result_value = int(key(result))

                is_larger = result_value >= next_value
                is_smaller = result_value <= next_value

                if (is_desc and is_smaller) or (not is_desc and is_larger):
                    next_offset += 1
                else:
                    break
    else:
        next_value = value
        next_offset = offset
        has_next = False

    # If the cursor contains previous results, the first item is the item that
    # indicates if we have more items later, and is *not* the first item im the
    # list, that should be used for the value.
    first_prev_index = 1 if is_prev and has_prev else 0

    # If we're paging back we need to calculate the key from the first result
    # with for_prev=True to ensure rounding of the key is correct.See
    # sentry.api.paginator.BasePaginator.get_item_key
    prev_value = int(key(results[first_prev_index], for_prev=True)) if results else 0

    # Determine what our previous cursor is by ensuring we have a unique offset
    if is_prev and num_results:
        # If we don't have an earlier value just add the offset
        if prev_value == value:
            prev_offset = offset + limit
        else:
            # Just as above, we may need to add an offset if any of the results at
            # the beginning are *before* or *after* (depending on the is_desc
            # flag).
            #
            # This is required to account for loss of precision in the key value.
            prev_offset = 0
            result_iter = iter(results)

            # If we know there are more previous results, we need to move past
            # the item indicating that more items exist.
            if has_prev:
                six.next(result_iter)

            # Always move past the first item, this is the prev_value item and will
            # already be offset in the next query.
            six.next(result_iter)

            for result in result_iter:
                result_value = int(key(result, for_prev=True))

                is_larger = result_value >= prev_value
                is_smaller = result_value <= prev_value

                # Note that the checks are reversed here as a prev query has
                # it's ordering reversed.
                if (is_desc and is_larger) or (not is_desc and is_smaller):
                    prev_offset += 1
                else:
                    break
    else:
        # Prev only has an offset if the cursor we were dealing with was a
        # previous cursor. Otherwise we'd be taking the offset while moving forward.
        prev_offset = offset if is_prev else 0

    # Truncate the list to our original result size now that we've determined
    # the next/prev page
    if is_prev and has_prev:
        results = results[1:]
    elif not is_prev:
        results = results[:limit]

    next_cursor = Cursor(next_value or 0, next_offset, False, has_next)
    prev_cursor = Cursor(prev_value or 0, prev_offset, True, has_prev)

    return CursorResult(
        results=results,
        next=next_cursor,
        prev=prev_cursor,
        hits=hits,
        max_hits=max_hits,
    )
