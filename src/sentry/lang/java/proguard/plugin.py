from __future__ import absolute_import

import functools
import logging

from sentry.interfaces.exception import SingleException
from sentry.lang.java.proguard.reader import read, index
from sentry.models import Release, ReleaseFile, Project
from sentry.plugins import Plugin2
from sentry.interfaces.stacktrace import Stacktrace


logger = logging.getLogger(__name__)


def rewrite_frame(mappings, frame):
    module = mappings.get(frame.module)
    if module is not None:
        name, members = module
        frame.module = name

        function = members.get(frame.function)
        if function is not None:
            frame.function = function

    return frame


def rewrite_stacktrace(mappings, stacktrace):
    stacktrace.frames = map(
        functools.partial(rewrite_frame, mappings),
        stacktrace.frames,
    )
    return stacktrace


def rewrite_exception(mappings, exception):
    module = mappings.get('{}.{}'.format(exception.module, exception.type))
    if module is not None:
        exception.module, exception.type = module[0].rsplit('.', 1)

    if exception.stacktrace:
        exception.stacktrace = rewrite_stacktrace(
            mappings,
            exception.stacktrace,
        )

    return exception


def deobfuscate_event(data):
    project = Project.objects.get(id=data['project'])
    try:
        release = project.release_set.get(version=data['release'])
    except KeyError:
        logger.debug('Skipping event that is not associated with a release.')
        return data
    except Release.DoesNotExist:
        logger.debug('Skipping event that is associated with an invalid release.')
        return data

    try:
        file_wrapper = release.releasefile_set.select_related('file').get(name='mapping.txt').file
    except ReleaseFile.DoesNotExist:
        logger.debug('Skipping event that does not have an associated ProGuard mapping file.')
        return data

    mappings = index(read(file_wrapper.getfile()))

    exceptions = data.get('sentry.interfaces.Exception', {}).get('values', [])
    for i, exception in enumerate(exceptions):
        raw_stacktrace = exception.get('stacktrace')
        exception = rewrite_exception(
            mappings,
            SingleException.to_python(exception),
        ).to_json()
        if raw_stacktrace:
            exception['raw_stacktrace'] = raw_stacktrace
        exceptions[i] = exception

    stacktrace = data.get('sentry.interfaces.Stacktrace')
    if stacktrace is not None:
        data['sentry.interfaces.Stacktrace'] = rewrite_stacktrace(
            mappings,
            Stacktrace.to_python(stacktrace),
        ).to_json()

    return data


class ProguardDeobfuscationPlugin(Plugin2):
    def get_event_preprocessors(self, data, **kwargs):
        preprocessors = []
        if data.get('platform') == 'java':
            preprocessors.append(deobfuscate_event)
        return preprocessors
