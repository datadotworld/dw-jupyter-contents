# dwcontents
# Copyright 2018 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).
from __future__ import unicode_literals

import base64
from builtins import str
from functools import reduce
from time import sleep

import backoff
import requests
from future.moves.urllib.parse import quote
from requests import Request, Session
from requests.adapters import BaseAdapter, HTTPAdapter
from tornado.web import HTTPError

from dwcontents import __version__
from dwcontents.utils import unique_justseen, directory_path, to_nb_json, MWT

str('Use str() once to force PyCharm to keep import')

MAX_TRIES = 10  # necessary to configure backoff decorator
CACHE_TIMEOUT = 30


def to_endpoint_url(endpoint):
    return 'https://api.data.world/v0{}'.format(endpoint)


def is_dataset_ready(d):
    files = d.get('files', []) if d is not None else []

    def is_file_ready(cur_file):
        file_source = cur_file.get('source', {})
        sync_status = file_source.get('syncStatus')
        return (cur_file.get('sizeInBytes') is not None or
                sync_status not in [None, 'NEW', 'INPROGRESS'])

    return reduce(lambda ready, cur_file: ready and is_file_ready(cur_file),
                  files, True)


def map_exceptions(fn):
    def decorated(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except requests.HTTPError as e:
            try:
                raise HTTPError(
                    e.response.status_code,
                    log_message=e.response.json()['message'],
                    reason=e.response.json()['message']
                )
            except (KeyError, ValueError):
                raise HTTPError(
                    e.response.status_code,
                    log_message=e.response.reason,
                    reason=e.response.reason)
        except UnicodeDecodeError:
            raise HTTPError(400, log_message='Bad format', reason='Bad format')

    decorated.__doc__ = fn.__doc__
    return decorated


class DwContentsApi(object):
    def __init__(self, api_token):
        self._session = Session()
        default_headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(api_token),
            'Content-Type': 'application/json',
            'User-Agent': 'dw-jupyter-contents - {}'.format(__version__)
        }
        self._session.headers.update(default_headers)
        self._session.mount('https://api.data.world/v0',
                            BackoffAdapter(HTTPAdapter()))

    @MWT(timeout=CACHE_TIMEOUT)
    @map_exceptions
    def get_me(self):
        resp = self._session.get(
            to_endpoint_url('/user')
        )
        resp.raise_for_status()
        return resp.json()

    @MWT(timeout=CACHE_TIMEOUT)
    @map_exceptions
    def get_user(self, user):
        resp = self._session.get(
            to_endpoint_url('/users/{}'.format(user))
        )
        if resp.status_code in [400, 404]:
            return None
        else:
            resp.raise_for_status()
            return resp.json()

    @MWT(timeout=CACHE_TIMEOUT)
    @map_exceptions
    @backoff.on_predicate(
        backoff.expo,
        predicate=lambda d: not (is_dataset_ready(d)),
        max_tries=lambda: MAX_TRIES,
        factor=0.1)
    def get_dataset(self, owner, dataset_id):
        resp = self._session.get(
            to_endpoint_url('/datasets/{}/{}'.format(owner, dataset_id)),
        )
        if resp.status_code in [400, 404]:
            return None
        else:
            resp.raise_for_status()
            return resp.json()

    @MWT(timeout=CACHE_TIMEOUT)
    @map_exceptions
    def get_datasets(self):
        def get(scope):
            req = Request(
                method='GET',
                url=to_endpoint_url('/user/datasets/{}'.format(scope)),
                params={'limit': 100, 'fields': 'id,owner,title,accessLevel,'
                                                'created,updated'}
            )

            dataset_pages = [page for page in self._paginate(req)]

            return dataset_pages

        pages = get('own') + get('contributing') + get('liked')
        datasets = [d for page in pages for d in page]

        return list(unique_justseen(
            datasets,
            key=lambda d: (d['owner'], d['id'])))

    @map_exceptions
    def get_file(self, owner, dataset_id, file_name, format='json'):
        resp = self._session.get(
            to_endpoint_url('/file_download/{}/{}/{}'.format(
                owner, dataset_id, quote(file_name, safe='')
            ))
        )
        resp.raise_for_status()
        return self._decode_response(resp, format)

    @map_exceptions
    def upload_file(self, owner, dataset_id, file_name, data):
        # TODO Fix API (support for files in subdirectories)
        resp = self._session.put(
            to_endpoint_url('/uploads/{}/{}/files/{}'.format(
                owner, dataset_id, quote(file_name, safe='')
            )),
            data=data,
            headers={'Content-Type': 'application/octet-stream'})
        resp.raise_for_status()
        MWT().invalidate()
        return self.get_dataset(owner, dataset_id)

    @map_exceptions
    def delete_subdirectory(self, owner, dataset_id, directory_name):
        dataset = self.get_dataset(owner, dataset_id)
        for f in dataset.get('files', []):
            if f['name'].startswith(directory_path(directory_name)):
                self.delete_file(owner, dataset_id, f['name'])
        MWT().invalidate()

    @map_exceptions
    def delete_file(self, owner, dataset_id, file_name):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}/files/{}'.format(
                owner, dataset_id, quote(file_name, safe='')))
        ).raise_for_status()
        MWT().invalidate()

    @map_exceptions
    def delete_dataset(self, owner, dataset_id):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}'.format(owner, dataset_id))
        ).raise_for_status()
        MWT().invalidate()

    def _decode_response(self, resp, format):
        if format == 'json':
            content = resp.json()
            nb = to_nb_json(content, version_specific=True)
            # TODO Harden and deal with version migrations
            return nb
        elif format == 'base64':
            return base64.b64encode(resp.content).decode('ascii')
        else:
            return resp.content.decode('utf-8')

    def _paginate(self, req):
        while True:
            prep_req = self._session.prepare_request(req)
            resp = self._session.send(prep_req)
            resp.raise_for_status()
            page = resp.json()
            yield page['records']
            if 'nextPageToken' in page:
                req.params['next'] = page['nextPageToken']
            else:
                break


class BackoffAdapter(BaseAdapter):
    def __init__(self, delegate):
        """Requests adapter for retrying throttled requests (HTTP 429)
        :param delegate: Adapter to delegate final request processing to
        :type delegate: requests.adapters.BaseAdapter
        """
        self._delegate = delegate
        super(BackoffAdapter, self).__init__()

    @backoff.on_predicate(backoff.expo,
                          predicate=lambda r: r.status_code == 429,
                          max_tries=lambda: MAX_TRIES)
    def send(self, request, **kwargs):
        resp = self._delegate.send(request, **kwargs)
        if (resp.status_code == 429 and
                resp.headers.get('Retry-After')):
            sleep(int(resp.headers.get('Retry-After')))

        return resp

    def close(self):
        self._delegate.close()
