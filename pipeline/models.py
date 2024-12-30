from mongoengine import Document, StringField, ListField, DateTimeField, \
    QuerySet, IntField, EmbeddedDocumentField, EmbeddedDocument, FloatField


class HostQuerySet(QuerySet):
    def aggregate_by_hostname(self):
        pipeline = [
            {
                '$sort': {
                    'discoveredAt': -1
                }
            }, {
                '$group': {
                    '_id': {
                        'hostname': '$hostname'
                    },
                    'mergedDocument': {
                        '$mergeObjects': '$$ROOT'
                    },
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    'mergedDocument': 1,
                    'count': 1,
                    'daysSinceLastSeen': {
                        '$divide': [
                            {
                                '$subtract': [
                                    '$mergedDocument.createdAt', '$mergedDocument.lastSeenAt'
                                ]
                            }, 1000 * 60 * 60 * 24
                        ]
                    },
                    'daysSinceDiscovered': {
                        '$divide': [
                            {
                                '$subtract': [
                                    '$mergedDocument.createdAt', '$mergedDocument.discoveredAt'
                                ]
                            }, 1000 * 60 * 60 * 24
                        ]
                    }
                }
            },
            {
                '$merge': {
                    'into': 'merged_hosts',
                    'on': '_id',
                    'whenMatched': 'merge',
                    'whenNotMatched': 'insert'
                }
            }
        ]

        return Host.objects().aggregate(pipeline)


class HostBase:
    externalId = StringField(required=True, unique=True)
    publicIpAddress = StringField(required=True, max_length=50)
    privateIpAddress = StringField(max_length=50)
    hostname = StringField(required=True, max_length=50)
    biosDescription = StringField(max_length=50)
    cloudProvider = StringField(max_length=50)
    tags = ListField(StringField(max_length=30))
    os = StringField(required=True, max_length=96)
    platform = StringField(max_length=50)
    kernel = StringField(max_length=50)
    status = StringField(max_length=50)
    processor = ListField(StringField(max_length=96))
    accountId = StringField(max_length=50)
    policies = StringField(max_length=50)
    lastSeenAt = DateTimeField(required=True)
    discoveredAt = DateTimeField(required=True)
    createdAt = DateTimeField(auto_now_add=True, auto_now=False)

    def to_dict(self):
        return {
            'externalId': self.externalId,
            'publicIpAddress': self.publicIpAddress,
            'privateIpAddress': self.privateIpAddress,
            'hostname': self.hostname,
            'biosDescription': self.biosDescription,
            'cloudProvider': self.cloudProvider,
            'tags': self.tags,
            'os': self.os,
            'platform': self.platform,
            'kernel': self.kernel,
            'status': self.status,
            'processor': self.processor,
            'accountId': self.accountId,
            'policies': self.policies,
            'lastSeenAt': self.lastSeenAt,
            'discoveredAt': self.discoveredAt,
            'createdAt': self.createdAt,
        }


class Host(Document, HostBase):
    meta = {
        'queryset_class': HostQuerySet
    }


class HostEmbedded(EmbeddedDocument, HostBase):
    _id = StringField()
    isMerged = StringField()


class MergedHost(Document):
    meta = {'collection': 'merged_hosts'}
    mergedDocument = EmbeddedDocumentField(HostEmbedded)
    count = IntField()
    daysSinceDiscovered = FloatField()
    daysSinceLastSeen = FloatField()
