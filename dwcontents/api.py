import base64
from time import sleep

import backoff
from requests import Request, Session
from requests.adapters import BaseAdapter, HTTPAdapter

from dwcontents import __version__
from dwcontents.utils import unique_justseen

MAX_TRIES = 10  # necessary to configure backoff decorator


def to_endpoint_url(endpoint):
    return 'https://api.data.world/v0{}'.format(endpoint)


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
            to_endpoint_url('/user/{}'.format(user))
        )
        if resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()
            return resp.json()

    @backoff.on_predicate(
        backoff.expo,
        predicate=lambda d: d['status'] not in ['LOADED', 'SYSTEMERROR'],
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
                                               'created,updated'}
            )

            dataset_pages = [page for page in self._paginate(req)]

            return dataset_pages

        pages = get('own') + get('contributing') + get('liked')
        datasets = [d for page in pages for d in page]

        return sorted(
            unique_justseen(
                datasets,
                key=lambda d: (d['owner'], d['id'])),
            key=lambda d: (d['owner'], d['title']))

    def get_file(self, owner, dataset_id, file_name, format='json'):
        # TODO test file name encoding (incl. subdirs)
        resp = self._session.get(
            to_endpoint_url('/file_download/{}/{}/{}'.format(
                owner, dataset_id, file_name
            ))
        )
        resp.raise_for_status()
        if format == 'json':
            return resp.json()
        elif format == 'base64':
            return base64.b64encode(resp.content).decode('ascii')
        else:
            return resp.content.decode('utf-8')

    def upload_file(self, owner, dataset_id, file_name, data):
        # TODO test file name encoding (incl. subdirs)
        resp = self._session.put(
            to_endpoint_url('/uploads/{}/{}/files/{}'.format(
                owner, dataset_id, file_name
            )),
            data=data,
            headers={'Content-Type': 'application/octet-stream'})
        resp.raise_for_status()
        return self.get_dataset(owner, dataset_id)

    def delete_file(self, owner, dataset_id, file_name):
        # TODO test file name encoding (incl. subdirs)
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}/files/{}'.format(
                owner, dataset_id, file_name))
        ).raise_for_status()

    def delete_dataset(self, owner, dataset_id):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}'.format(owner, dataset_id))
        ).raise_for_status()

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

    def _poll_until(self, req, test_func,
                    sleep_secs=1, max_retries=60 * 60):
        retries = max_retries
        prep_request = self._session.prepare_request(req)
        resp = self._session.send(prep_request)
        while not test_func(resp):
            retries = retries - 1
            if retries <= 0:
                raise TimeoutError()  # TODO specify message
            sleep(sleep_secs)
            resp = self._session.send(prep_request)
            resp.raise_for_status()
        return resp


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