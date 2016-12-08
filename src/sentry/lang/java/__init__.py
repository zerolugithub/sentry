from __future__ import absolute_import

from sentry.lang.java.proguard.plugin import ProguardDeobfuscationPlugin
from sentry.plugins import register


register(ProguardDeobfuscationPlugin)
