from pydash import get as get_by_rule, map_ as pymap
from datetime import datetime
from flask import current_app
from .abstract_provider import AbstractProvider
from ..models import Host


class Qualys(AbstractProvider):
    """
    Qualys entity model class
    """

    def __init__(self):
        endpoint = current_app.config.get('QUALYS_ENDPOINT')
        secret = current_app.config.get('SECRET')
        super(Qualys, self).__init__(endpoint, secret)

    def from_data(self, data) -> Host:
        host = Host()
        host.externalId = str(get_by_rule(data, 'id'))
        host.publicIpAddress = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.publicIpAddress')
        host.privateIpAddress = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.privateIpAddress')
        host.hostname = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.localHostname')
        host.biosDescription = get_by_rule(data, 'biosDescription')
        host.cloudProvider = get_by_rule(data, 'cloudProvider')
        host.tags = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.ec2InstanceTags.tags.list')
        host.os = get_by_rule(data, 'os')
        host.platform = get_by_rule(data, 'agentInfo.platform')
        host.status = get_by_rule(data, 'agentInfo.status')
        host.processor = pymap(data.get('processor', {}).get('list', []), 'HostAssetProcessor.name')
        host.accountId = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.accountId')
        host.lastSeenAt = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.lastUpdated')
        host.discoveredAt = get_by_rule(data, 'sourceInfo.list[0].Ec2AssetSourceSimple.firstDiscovered')
        host.createdAt = datetime.now()

        return host
