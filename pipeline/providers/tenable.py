import logging

from pydash import get as get_by_rule
from .abstract_provider import AbstractProvider
from ..models import Host
from datetime import datetime, timezone
from requests import post
from flask import current_app
from typing import Dict, List
from mongoengine import BulkWriteError, ValidationError, NotUniqueError


class Tenable(AbstractProvider):
    """
    Tenable entity model class
    """

    def __init__(self) -> None:
        endpoint = current_app.config.get('TENABLE_ENDPOINT')
        secret = current_app.config.get('SECRET')
        super(Tenable, self).__init__(endpoint, secret)

    def from_data(self, data) -> Host:
        host = Host()
        host.externalId = str(get_by_rule(data, '_id'))
        host.publicIpAddress = get_by_rule(data, 'display_ipv4_address')
        # host.privateIpAddress = get_by_rule(data, 'connection_ip')
        # host.hostname = get_by_rule(data, 'hostname')
        # host.biosDescription = f"{get_by_rule(data, 'bios_manufacturer')} {get_by_rule(data, 'bios_version')}"
        # host.cloudProvider = get_by_rule(data, 'service_provider')
        # host.tags = get_by_rule(data, 'tags.list')
        # host.os = get_by_rule(data, 'os_version')
        # host.platform = get_by_rule(data, 'platform_name')
        # host.kernel = get_by_rule(data, 'kernel_version')
        # host.status = get_by_rule(data, 'status')
        # host.accountId = get_by_rule(data, 'service_provider_account_id')
        # host.lastSeenAt = get_by_rule(data, 'last_seen')
        # host.discoveredAt = get_by_rule(data, 'first_seen')
        # host.createdAt = host.createdAt = datetime.now(timezone.utc)

        return host

    def get_data(self, cursor) -> List[Dict[str, dict]]:
        response = post(f"{self.endpoint}?cursor={cursor}", headers=self.headers)
        response.raise_for_status()
        data = response.json()
        return data

    def fetch(self, skip, limit) -> List[Host]:
        results = []
        cursor_current = None
        cursor_resume = True
        while cursor_resume:
            (hosts, cursor) = self.get_data(cursor_current)
            cursor_resume = cursor != cursor_current
            cursor_current = cursor
            for item in self.get_data(hosts):
                entity = self.from_data(item)
                try:
                    entity.validate()
                    results.append(entity)
                except ValidationError as ve:
                    logging.error('%s validate error', ve)

            results.append(hosts)

        if results:
            try:
                Host.objects.insert(results, load_bulk=True)
            except BulkWriteError as e:
                self.single_insert(results)
                logging.warning('%s duplicate detected', e)

        return results
