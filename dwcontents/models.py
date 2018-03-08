from itertools import groupby


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


def valid_file(file):
    print('Checking {}'.format(file['name']))
    return {'created', 'updated'}.issubset(set(file.keys()))


def relative_path(file, parent):
    return file[len(parent):].strip('/')


def map_root(me, datasets=None, include_content=False):
    root = {
        'name': '',
        'path': '',
        'writable': False,
        'type': 'directory',
        'mimetype': None,
        'format': 'json',
        'created': me['created'],
        'updated': me['updated']
    }
    if datasets is not None:
        content = map_accounts(datasets)
        if include_content:
            root['content'] = content
        root.update(reduce_dates(content))

    return root


def map_accounts(datasets):
    sorted_datasets = sorted(datasets, key=lambda d: d['owner'])
    accounts = groupby(sorted_datasets, lambda d: d['owner'])
    content = [map_account(a, list(ds))
               for a, ds in accounts]

    return content


def map_account(account, datasets, include_content=False):
    account_entity = {
        'name': account,
        'path': account,
        'writable': True,  # TODO confirm
        'type': 'directory',
        'mimetype': None,
        'format': 'json',
    }
    content = map_datasets(datasets)
    if include_content:
        account_entity['content'] = content
    account_entity.update(reduce_dates(content))

    return account_entity


def map_datasets(datasets):
    return [map_dataset(d) for d in datasets]


def map_dataset(dataset, include_content=False):
    dataset_entity = {
        'name': dataset['title'],
        'path': '{}/{}'.format(dataset['owner'], dataset['id']),
        'writable': dataset.get('accessLevel') in ['WRITE', 'ADMIN'],
        'type': 'directory',
        'mimetype': None,
        'format': 'json',
        'created': dataset['created'],
        'last_modified': dataset['updated']
    }

    if include_content:
        dataset_entity['content'] = map_items(dataset)
    return dataset_entity


def map_items(dataset, subdir=''):
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

    print('Files {}'.format(files))
    print('Subdirs {}'.format(subdirs))

    return (map_subdirs(subdirs, subdir, dataset) +
            map_files(files, subdir, dataset))


def map_subdirs(subdirs, parent, dataset):
    return [map_subdir(s, parent, dataset) for s in subdirs]


def map_subdir(subdir, parent, dataset, include_content=False):
    subdir_entity = {
        'name': subdir,
        'path': '{}/{}/{}/{}'.format(dataset['owner'], dataset['id'],
                                     parent, subdir),
        'writable': dataset.get('accessLevel') in ['WRITE', 'ADMIN'],
        'type': 'directory',
        'mimetype': None,
        'format': 'json',
        'created': dataset['created'],
        'last_modified': dataset['updated']
    }
    if include_content:
        subdir_entity['content'] = map_items(dataset, subdir)

    return subdir_entity


def map_files(files, parent, dataset):
    return [map_file(file, parent, dataset) for file in files]


def map_file(file, parent, dataset, include_content=False):
    file_name = relative_path(file['name'], parent)
    is_notebook = file_name.endswith('.ipynb')
    file_entity = {
        'name': file_name,
        'path': '{}/{}/{}'.format(dataset['owner'], dataset['id'],
                                  file['name']),
        'writable': dataset.get('accessLevel') in ['WRITE', 'ADMIN'],
        'type': 'notebook' if is_notebook else 'file',
        'mimetype': None if is_notebook else 'application/octet-stream',
        'format': 'json' if is_notebook else 'base64',
        'created': file['created'],
        'last_modified': file['updated']
    }
    if include_content:
        # TODO download content
        pass
    return file_entity
