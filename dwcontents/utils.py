from itertools import groupby
from operator import itemgetter


def directory_path(path):
    path = normalize_path(path)
    return path if path == '' else '{}/'.format(path)


def normalize_path(*parts):
    return '/'.join([part.strip('/') for part in parts if part != ''])


def relative_path(path, parent):
    path = normalize_path(path)
    parent = normalize_path(parent)
    return normalize_path(path[len(parent):])


def split_parent(path):
    path = normalize_path(path)
    parent, _, name = path.rpartition('/')
    return directory_path(parent), name


def to_api_path(dw_path, root_dir='', prefix=''):
    rel_path = relative_path(dw_path, root_dir)
    if prefix != '':
        # TODO Implement and test prefixes (hybrid contents manager)
        return normalize_path(prefix, rel_path)
    else:
        return rel_path


def to_dw_path(path, root_dir='', prefix=''):
    path = normalize_path(path)
    if path == prefix:
        path = root_dir
    else:
        path = normalize_path(
            root_dir, relative_path(path, prefix))

    path_parts = path.split('/', 2)

    owner = path_parts[0] if path_parts[0] != '' else None
    dataset_id = path_parts[1] if len(path_parts) > 1 else None
    file_path = path_parts[2] if len(path_parts) > 2 else None

    return owner, dataset_id, file_path


def unique_justseen(iterable, key=None):
    return map(next, map(itemgetter(1),
                         groupby(sorted(iterable, key=key), key)))
