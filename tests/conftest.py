import copy
import datetime
import json
from collections import namedtuple
from itertools import groupby

from pytest import fixture

from dwcontents.api import DwContentsApi
from dwcontents.utils import split_parent


class InMemDwContentsApi(DwContentsApi):
    def __init__(self):
        self.dataset = {
            'owner': 'testy-tester',
            'id': 'jupyter',
            'accessLevel': 'WRITE',
            'visibility': 'PRIVATE',
            'created': datetime.datetime.now().isoformat(),
            'updated': datetime.datetime.now().isoformat(),
            'status': 'LOADED'
        }
        self.file_data = {}

    def delete_directory(self, owner, dataset_id, directory_name):
        directory_name = (directory_name
                          if directory_name == ''
                          else '{}/'.format(directory_name))
        to_be_deleted = [f['name'] for f in self.dataset.get('files', [])
                         if f['name'].startswith(directory_name)]
        remaining = [
            f for f in self.dataset.get('files', [])
            if f['name'] not in to_be_deleted]

        self.dataset['files'] = remaining

    def delete_file(self, owner, dataset_id, file_name):
        self.dataset['files'] = [
            f for f in self.dataset.get('files', [])
            if f['name'] != file_name]

    def get_dataset(self, owner, dataset_id):
        return self.dataset_nodummies

    def get_datasets(self):
        return [self.dataset_nodummies]

    def get_file(self, owner, dataset_id, file_name, format='json'):
        Response = namedtuple('Response', ['json', 'content'])
        return self._decode_response(Response(
            json=lambda: json.loads(self.file_data[file_name]),
            content=self.file_data[file_name]), format)

    def get_me(self):
        return {'id': 'testy-tester'}

    def get_user(self, user):
        return {'id': user}

    def upload_file(self, owner, dataset_id, file_name, data):
        self.delete_file(owner, dataset_id, file_name)
        self.dataset['files'] = (
            self.dataset.get('files', []) +
            [{'name': file_name,
              'sizeInBytes': 10,
              'created': datetime.datetime.now().isoformat(),
              'updated': datetime.datetime.now().isoformat()}])
        self.file_data[file_name] = data
        return self.dataset

    @property
    def dataset_nodummies(self):
        ds_nd = copy.copy(self.dataset)

        directories = groupby(
            sorted(ds_nd.get('files', []), key=lambda f: f['name']),
            key=lambda f: split_parent(f['name'])[0])

        ds_nd['files'] = []
        for _, d in directories:
            files = list(d)
            for f in files:
                if not f['name'].endswith('dummy') or len(files) <= 1:
                    ds_nd['files'].append(f)

        return ds_nd


@fixture(scope='class')
def api_class(request):
    request.cls.api_class = InMemDwContentsApi
