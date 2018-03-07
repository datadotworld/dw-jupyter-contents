from time import sleep

import backoff
from requests import Request, Session
from requests.adapters import BaseAdapter, HTTPAdapter

from dwcontents import __version__

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

    def get_dataset(self, owner, id):
        resp = self._session.get(
            to_endpoint_url('/datasets/{}/{}'.format(owner, id))
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
                # TODO Specify fields in params when accessLevel is supported
                params={'limit': 50}
            )

            dataset_pages = [page for page in self._paginate(req)]

            return dataset_pages

        pages = get('own') + get('contributing')
        datasets = [d for page in pages for d in page]
        datasets_rw = [d for d in datasets
                       if d['accessLevel'] in ['WRITE', 'ADMIN']]

        return sorted(datasets_rw,
                      key=lambda d: '{}/{}'.format(d['owner'], d['title']))

    def delete_file(self, owner, id, name):
        self._session.delete(
            to_endpoint_url(
                '/datasets/{}/{}/files/{}'.format(owner, id, name)),
            headers={'Authorization': 'Bearer {}'.format(self.api_token)}
        ).raise_for_status()

    def delete_dataset(self, owner, id):
        self._session.delete(
            to_endpoint_url('/datasets/{}/{}'.format(owner, id)),
            headers={'Authorization': 'Bearer {}'.format(self.api_token)}
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
