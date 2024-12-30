from pydash import get as get_by_rule
from .abstract_provider import AbstractProvider
from ..models import Host
from datetime import datetime, timezone
from flask import current_app


class Crowdstrike(AbstractProvider):
    """
    Crowdstrike entity model class
    """

    def __init__(self) -> None:
        endpoint = current_app.config.get('CROWDSTRIKE_ENDPOINT')
        secret = current_app.config.get('SECRET')
        super(Crowdstrike, self).__init__(endpoint, secret)

    def from_data(self, data) -> Host:
        host = Host()
        host.externalId = str(get_by_rule(data, '_id'))
        host.publicIpAddress = get_by_rule(data, 'external_ip')
        host.privateIpAddress = get_by_rule(data, 'connection_ip')
        host.hostname = get_by_rule(data, 'hostname')
        host.biosDescription = f"{get_by_rule(data, 'bios_manufacturer')} {get_by_rule(data, 'bios_version')}"
        host.cloudProvider = get_by_rule(data, 'service_provider')
        host.tags = get_by_rule(data, 'tags.list')
        host.os = get_by_rule(data, 'os_version')
        host.platform = get_by_rule(data, 'platform_name')
        host.kernel = get_by_rule(data, 'kernel_version')
        host.status = get_by_rule(data, 'status')
        host.accountId = get_by_rule(data, 'service_provider_account_id')
        host.lastSeenAt = get_by_rule(data, 'last_seen')
        host.discoveredAt = get_by_rule(data, 'first_seen')
        host.createdAt = host.createdAt = datetime.now(timezone.utc)

        return host
