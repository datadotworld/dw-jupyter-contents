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
from __future__ import unicode_literals, print_function

from builtins import str
from itertools import groupby

import nbformat
from nbformat import v1, v2, v3, v4

str('Use str() once to force PyCharm to keep import')


def directory_path(path):
    path = normalize_path(path)
    return path if path == '' else '{}/'.format(path)


def normalize_path(*parts):
    return '/'.join([part.strip('/') for part in parts
                     if part is not None and part != ''])


def relative_path(path, parent):
    path = normalize_path(path)
    parent = normalize_path(parent)
    return normalize_path(path[len(parent):])


def split_parent(path):
    path = normalize_path(path)
    parent, _, name = path.rpartition('/')
    return directory_path(parent), name


def to_api_path(dw_path, root_dir=''):
    rel_path = relative_path(dw_path, root_dir)
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


def to_nb_json(content, version_specific=False):
    if not version_specific:
        return nbformat.from_dict(content)
    else:
        # Not sure why this is needed instead of from_dict, sometimes
        versions = {
            1: v1,
            2: v2,
            3: v3,
            4: v4,
        }
        major = content.get('nbformat', 1)
        minor = content.get('nbformat_minor', 0)
        nb = versions[major].to_notebook_json(content, minor=minor)
        return nb


def unique_justseen(iterable, key=None):
    sorted_items = sorted(iterable, key=key)
    groups = groupby(sorted_items, key=key)
    return (next(v) for k, v in groups)
