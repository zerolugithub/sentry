from __future__ import absolute_import

from rest_framework.response import Response

from sentry.api.bases.organization import OrganizationEndpoint
from sentry.plugins import bindings
from sentry import features


class OrganizationConfigRepositoriesEndpoint(OrganizationEndpoint):
    def has_access(self, organization, provider_id):
        if provider_id == 'bitbucket' and not features.has(
            'organizations:bitbucket-repos', organization
        ):
            return False

        # TODO(jess): figure out better way to exclude this
        if provider_id == 'github_apps':
            return False

        return True

    def get(self, request, organization):
        provider_bindings = bindings.get('repository.provider')

        providers = []
        for provider_id in provider_bindings:
            provider = provider_bindings.get(provider_id)(id=provider_id)
            if self.has_access(organization, provider_id):
                providers.append(
                    {
                        'id': provider_id,
                        'name': provider.name,
                        'config': provider.get_config(),
                    }
                )

        return Response({
            'providers': providers,
        })
