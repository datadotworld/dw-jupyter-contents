import base64
from functools import reduce
from time import sleep

import backoff
from future.moves.urllib.parse import quote
from nbformat import v1, v2, v3, v4
from requests import Request, Session
from requests.adapters import BaseAdapter, HTTPAdapter

from dwcontents import __version__
from dwcontents.utils import unique_justseen, directory_path

MAX_TRIES = 10  # necessary to configure backoff decorator


def to_endpoint_url(endpoint):
    return 'https://api.data.world/v0{}'.format(endpoint)


versions = {
    1: v1,
    2: v2,
    3: v3,
    4: v4,
}


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

    def get_me(self):
        resp = self._session.get(
            to_endpoint_url('/user')
        )
        resp.raise_for_status()
        return resp.json()

    def get_user(self, user):
        resp = self._session.get(
            to_endpoint_url('/users/{}'.format(user))
        )
        if resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()
            return resp.json()

    @backoff.on_predicate(
        backoff.expo,
        predicate=lambda d: reduce(
            lambda r, f: r or f.get('sizeInBytes') is None,
            d.get('files', []), False),
        max_tries=lambda: MAX_TRIES)
    def get_dataset(self, owner, dataset_id):
        resp = self._session.get(
            to_endpoint_url('/datasets/{}/{}'.format(owner, dataset_id)),
        )
        if resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()
            return resp.json()

    def get_datasets(self):
        def get(scope):
            req = Request(
                method='GET',
                url=to_endpoint_url('/user/datasets/{}'.format(scope)),
                # TODO Fix API (accessLevel missing)
                params={'limit': 50, 'fields': 'id,title,'
                                               'accessLevel,created,updated'}
            )

            dataset_pages = [page for page in self._paginate(req)]

            return dataset_pages

        pages = get('own') + get('contributing') + get('liked')
        datasets = [d for page in pages for d in page]

        return list(unique_justseen(
            datasets,
            key=lambda d: (d['owner'], d['id'])))

    def get_file(self, owner, dataset_id, file_name, format='json'):
        resp = self._session.get(
            to_endpoint_url('/file_download/{}/{}/{}'.format(
                owner, dataset_id, quote(file_name, safe='')
            ))
        )
        resp.raise_for_status()
        return self._decode_response(resp, format)

    def upload_file(self, owner, dataset_id, file_name, data):
        # TODO Fix API (support for files in subdirectories)
        resp = self._session.put(
            to_endpoint_url('/uploads/{}/{}/files/{}'.format(
                owner, dataset_id, quote(file_name, safe='')
            )),
            data=data,
            headers={'Content-Type': 'application/octet-stream'})
        resp.raise_for_status()
        return self.get_dataset(owner, dataset_id)

    def delete_directory(self, owner, dataset_id, directory_name):
        dataset = self.get_dataset(owner, dataset_id)
        for f in dataset.get('files', []):
            if f['name'].startswith(directory_path(directory_name)):
                self.delete_file(owner, dataset_id, f['name'])

    def delete_file(self, owner, dataset_id, file_name):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}/files/{}'.format(
                owner, dataset_id, quote(file_name, safe='')))
        ).raise_for_status()

    def delete_dataset(self, owner, dataset_id):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}'.format(owner, dataset_id))
        ).raise_for_status()

    def _decode_response(self, resp, format):
        if format == 'json':
            content = resp.json()
            major = content.get('nbformat', 1)
            minor = content.get('nbformat_minor', 0)
            nb = versions[major].to_notebook_json(content, minor=minor)
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
