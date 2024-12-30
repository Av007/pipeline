from requests import post
from abc import ABC, abstractmethod
from mongoengine import BulkWriteError, ValidationError, NotUniqueError
from ..models import Host
import logging
from typing import Dict, List


class AbstractProvider(ABC):
    """Abstract method that requests API using Semaphore Threads"""

    def __init__(self, endpoint, secret) -> None:
        self.endpoint = endpoint
        self.headers = {
            'accept': 'application/json',
            'token': secret
        }

    def get_data(self, skip, limit) -> List[Dict[str, dict]]:
        response = post(f"{self.endpoint}?skip={skip}&limit={limit}", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def fetch(self, skip, limit) -> List[Host]:
        results = []
        for item in self.get_data(skip, limit):
            entity = self.from_data(item)
            try:
                entity.validate()
                results.append(entity)
            except ValidationError as ve:
                logging.error('%s validate error', ve)

        if results:
            try:
                Host.objects.insert(results, load_bulk=True)
            except BulkWriteError as e:
                self.single_insert(results)
                logging.warning('%s duplicate detected', e)

        return results

    def single_insert(self, data) -> None:
        for item in data:
            try:
                item.save()
            except NotUniqueError as e:
                logging.warning('%s single duplicate error', e)

    @abstractmethod
    def from_data(self, data) -> Host:
        pass
