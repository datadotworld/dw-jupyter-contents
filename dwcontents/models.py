from functools import reduce
from itertools import groupby

import logging
import nbformat
from nbformat import v1, v2, v3, v4

versions = {
    1: v1,
    2: v2,
    3: v3,
    4: v4,
}


def guess_type(path, dir_exists_func=None):
    if path.endswith('.ipynb'):
        return 'notebook', 'json', None
    elif dir_exists_func is not None and dir_exists_func(path):
        return 'directory', 'json', None
    else:
        return 'file', 'base64', 'application/octet-stream'


def reduce_dates(entities):
    return reduce(lambda result, entity: result.update({
        'created': (
            result['created']
            if 'created' in result
               and result['created'] < entity['created']
            else entity['created']),
        'last_modified': (
            result['last_modified']
            if 'last_modified' in result
               and result['last_modified'] > entity['last_modified']
            else entity['last_modified'])
    }) or result, entities, {})


def relative_path(file, parent):
    return file[len(parent):].strip('/')


def valid_file(file):
    return {'created', 'updated'}.issubset(set(file.keys()))


class DwMapper(object):
    def __init__(self, prefix='', root_dir='', logger=None):
        self.root_dir = root_dir.strip('/')
        self.prefix = prefix.strip('/')
        self.log = (logger
                    if logger is not None else logging.getLogger('dwcontents'))

    def map_root(self, me, datasets=None, include_content=False):
        self.log.debug('[map_root] me:{} d(count):{} c:{}'.format(
            me['id'], len(datasets), include_content))
        root = {
            'name': '',
            'path': '',
            'writable': False,
            'type': 'directory',
            'content': None,
            'mimetype': None,
            'format': None,
            'created': me['created'],
            'updated': me['updated']
        }
        if datasets is not None:
            content = self.map_accounts(datasets)
            if include_content:
                root['content'] = content
                root['format'] = 'json'
            root.update(reduce_dates(content))

        return root

    def map_accounts(self, datasets):
        self.log.debug('[map_accounts] d(count):{}'.format(len(datasets)))
        sorted_datasets = sorted(datasets, key=lambda d: d['owner'])
        accounts = groupby(sorted_datasets, lambda d: d['owner'])
        content = [self.map_account(a, datasets=list(ds))
                   for a, ds in accounts]

        return content

    def map_account(self, account, datasets, include_content=False):
        self.log.debug('[map_account] a:{} d(count):{} c:{}'.format(
            account, len(datasets), include_content))
        account_entity = {
            'name': account,
            'path': self._api_path(account),
            'writable': False,
            'type': 'directory',
            'content': None,
            'mimetype': None,
            'format': None,
        }
        content = self.map_datasets(datasets)
        if include_content:
            account_entity['content'] = content
            account_entity['format'] = 'json'
        account_entity.update(reduce_dates(content))

        return account_entity

    def map_datasets(self, datasets):
        self.log.debug('[map_datasets] d(count):{}'.format(len(datasets)))
        return [self.map_dataset(d) for d in datasets]

    def map_dataset(self, dataset, include_content=False):
        self.log.debug('[map_dataset] d:{} c:{}'.format(dataset['id'],
                                                        include_content))
        dataset_entity = {
            'name': dataset['id'],
            'path': self._api_path(
                '{}/{}'.format(dataset['owner'], dataset['id'])),
            'writable': dataset.get('accessLevel') in ['WRITE', 'ADMIN'],
            'type': 'directory',
            'content': None,
            'mimetype': None,
            'format': None,
            'created': dataset['created'],
            'last_modified': dataset['updated']
        }

        if include_content:
            dataset_entity['content'] = self.map_items(dataset)
            dataset_entity['format'] = 'json'
        return dataset_entity

    def map_items(self, dataset, subdir=''):
        self.log.debug('[map_items] d:{} s{}'.format(dataset['id'], subdir))
        sorted_files = sorted(
            [f for f in dataset['files']
             if valid_file(f) and f['name'].startswith(subdir)],
            key=lambda f: f['name'])

        subdir_items = {k: list(g) for k, g in groupby(
            sorted_files,
            lambda f: relative_path(f['name'], subdir).partition('/')[0])}

        files = [children[0]
                 for path, children in subdir_items.items()
                 if len(children) == 1 and
                 path == relative_path(children[0]['name'], subdir)]

        subdirs = [path for path, children in subdir_items.items()
                   if not (len(children) == 1 and
                           path == relative_path(children[0]['name'], subdir))]

        return (self.map_subdirs(subdirs, parent=subdir, dataset_obj=dataset) +
                self.map_files(files, parent=subdir, dataset_obj=dataset))

    def map_subdirs(self, subdirs, parent, dataset_obj):
        self.log.debug('[map_subdirs] s(count):{} p:{} d:{}'.format(
            len(subdirs), parent, dataset_obj['id']))
        return [self.map_subdir(s, parent=parent, dataset_obj=dataset_obj)
                for s in subdirs]

    def map_subdir(self, subdir, parent, dataset_obj, include_content=False):
        self.log.debug('[map_subdir] s:{} p:{} d:{} c:{}'.format(
            subdir, parent, dataset_obj['id'], include_content))
        subdir_entity = {
            'name': subdir,
            'path': self._api_path(
                '{}/{}/{}'.format(
                    dataset_obj['owner'], dataset_obj['id'],
                    subdir if parent == '' else '{}/{}'.format(parent,
                                                               subdir))),
            'writable': dataset_obj.get('accessLevel') in ['WRITE', 'ADMIN'],
            'type': 'directory',
            'content': None,
            'mimetype': None,
            'format': None,
            'created': dataset_obj['created'],
            'last_modified': dataset_obj['updated']
        }
        if include_content:
            subdir_entity['content'] = self.map_items(dataset_obj,
                                                      subdir=subdir)
            subdir_entity['format'] = 'json'

        return subdir_entity

    def map_files(self, file_objs, parent, dataset_obj):
        self.log.debug('[map_files] f(count):{} p:{} d:{}'.format(
            len(file_objs), parent, dataset_obj['id']))
        return [self.map_file(file_obj, parent=parent, dataset_obj=dataset_obj)
                for file_obj in file_objs]

    def map_file(self, file_obj, parent, dataset_obj,
                 content_type=None, content_func=None):
        self.log.debug('[map_file] f:{} p:{} d:{} t:{} c:{}'.format(
            file_obj['name'], parent, dataset_obj['id'],
            content_type, content_func is not None))
        file_name = relative_path(file_obj['name'], parent)
        gtype, gformat, gmimetype = guess_type(file_obj['name'])
        if content_type is None:
            content_type = gtype
        file_entity = {
            'name': file_name,
            'path': self._api_path(
                '{}/{}/{}'.format(
                    dataset_obj['owner'], dataset_obj['id'],
                    file_obj['name'])),
            'writable': dataset_obj.get('accessLevel') in ['WRITE', 'ADMIN'],
            'created': file_obj['created'],
            'last_modified': file_obj['updated'],
            'content': None,
            'type': content_type,
            'mimetype': None,
            'format': None
        }

        if content_func is not None:
            content = content_func()
            if content_type == 'notebook':  # notebook
                # TODO Harden and deal with version migrations
                major = content.get('nbformat', 1)
                minor = content.get('nbformat_minor', 0)
                nb = versions[major].to_notebook_json(content, minor=minor)
                file_entity['content'] = nb
                file_entity['format'] = gformat
                nbformat.validate(file_entity['content'])
            else:
                file_entity['content'] = content
                file_entity['format'] = gformat
                file_entity['mimetype'] = gmimetype

        return file_entity

    def _api_path(self, dw_path):
        self.log.debug('[_api_path] dw_path:{}'.format(dw_path))
        rel_path = relative_path(dw_path, self.root_dir)
        if self.prefix != '':
            # TODO Implement and test prefixes (hybrid contents manager)
            return '{}/{}'.format(self.prefix, rel_path)
        else:
            return rel_path
