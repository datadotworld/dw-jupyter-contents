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

    def get_notebook(self, owner, dataset_id, file_name):
        resp = self._session.get(
            to_endpoint_url('/file_download/{}/{}/{}'.format(
                owner, dataset_id, file_name
            ))
        )
        resp.raise_for_status()
        notebook = resp.json()
        return notebook

    def delete_file(self, owner, dataset_id, file_name):
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
