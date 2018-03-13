import base64
import json
import os
import tempfile

from notebook.services.contents.filecheckpoints import GenericFileCheckpoints
from notebook.services.contents.manager import ContentsManager
from traitlets import Unicode

from dwcontents.api import DwContentsApi
from dwcontents.models import guess_type, DwMapper


class DwContents(ContentsManager):
    dw_auth_token = Unicode(
        allow_none=False,
        config=True,
        help="data.world API authentication token.",
    )

    def __init__(self, **kwargs):
        super(DwContents, self).__init__(**kwargs)

        os.environ['DW_AUTH_TOKEN'] = self.dw_auth_token
        self.api = DwContentsApi(self.dw_auth_token)
        self.mapper = DwMapper(root_dir=self.root_dir, logger=self.log)

        # TODO Support hybrid config

    def dir_exists(self, path):
        self.log.debug('[dir_exists] Checking {}'.format(path))
        owner, dataset_id, dir_path = self._to_dw_path(path)
        if dataset_id is None:
            if owner is None:
                # Root always exists
                return True
            else:
                user = self.api.get_user(owner)
                return user is not None
        else:
            dataset = self.api.get_dataset(owner, dataset_id)
            if dataset is None:
                return False
            elif dir_path is not None:
                for file in dataset['files']:
                    file_dir, _ = DwContents._split_parent(file['name'])
                    if file_dir.startswith(dir_path):
                        return True
                return False
            else:
                return True

    def file_exists(self, path=''):
        self.log.debug('[file_exists] Checking {}'.format(path))
        owner, dataset_id, file_path = self._to_dw_path(path)
        if owner is None or dataset_id is None:
            return False
        else:
            dataset = self.api.get_dataset(owner, dataset_id)
            if dataset is None:
                return False
            else:
                return self._get_file(dataset, file_path) is not None

    def get(self, path, content=True, type=None, format=None):
        self.log.debug('[get] Getting {}/{}/{}/{}'.format(
            path, content, type, format))

        owner, dataset_id, file_path = self._to_dw_path(path)
        if type is None:
            type, _, _ = guess_type(path, self.dir_exists)
            self.log.debug('[guess_type] Guessed {} type for {}'.format(
                type, path))

        if type == 'directory':
            if owner is None:
                # List root content
                return self.mapper.map_root(self.api.get_me(),
                                datasets=self.api.get_datasets(),
                                include_content=content)
            elif dataset_id is None:
                # List account content
                return self.mapper.map_account(
                    owner,
                    [d for d in self.api.get_datasets()
                     if d['owner'] == owner],
                    include_content=content)
            else:
                # List dataset content
                dataset = self.api.get_dataset(owner, dataset_id)
                if file_path is not None:
                    dir_parent, dir_name = DwContents._split_parent(file_path)
                    return self.mapper.map_subdir(
                        dir_name, dir_parent, dataset, include_content=content)
                else:
                    return self.mapper.map_dataset(
                        dataset, include_content=content)
        else:
            if file_path is None:
                raise DwError  # TODO Proper error

            dir_parent, _ = DwContents._split_parent(file_path)
            dataset = self.api.get_dataset(owner, dataset_id)
            file_obj = self._get_file(dataset, file_path)
            content_func = None
            if content:
                if type == 'notebook':
                    def content_func():
                        return self.api.get_file(
                            owner, dataset_id, file_path, 'json')
                else:
                    def content_func():
                        return self.api.get_file(
                            owner, dataset_id, file_path,
                            'base64' if format is None else format)
            return self.mapper.map_file(
                file_obj, dir_parent, dataset,
                content_type=type,
                content_func=content_func)

    def rename_file(self, old_path, new_path):
        self.log.debug('[rename_file] Renaming {} to {}'.format(
            old_path, new_path))

        owner, dataset_id, file_path = self._to_dw_path(new_path)
        if file_path is None:
            raise DwError()  # TODO Proper error

        if self.dir_exists(old_path):  # Is it a directory?
            # TODO Raise error if not testing
            return  # Moving dirs no-ops for testing

        old_file = self.get(old_path, content=True)
        self.save(old_file, new_path)
        self.delete_file(old_path)

    def save(self, model, path):
        self.log.debug('[save] Saving {} ({})'.format(path, model))
        self.run_pre_save_hook(model, path)

        owner, dataset_id, file_path = self._to_dw_path(path)
        file_dir, _ = DwContents._split_parent(file_path)

        model_type = model['type']
        if model_type == 'directory':
            if file_path is None:
                raise DwError()  # TODO Proper error
            else:
                # Saving dirs no-ops for testing
                # TODO Raise error if not testing
                return self.get(path, content=False, type='directory')
        else:
            if file_path is None:
                raise DwError()  # TODO Proper error

            if model_type == 'notebook':
                content = json.dumps(model['content']).encode('utf-8')
            else:
                model_format = model['format']
                if model_format == 'base64':
                    content = (base64
                               .b64decode(model['content']
                                          .encode('ascii')))
                else:
                    content = model['content'].encode('utf-8')

            updated_dataset = self.api.upload_file(
                owner, dataset_id, file_path,
                content)

            return self.mapper.map_file(
                self._get_file(updated_dataset, file_path),
                file_dir, updated_dataset,
                content_type=model_type)

    def delete_file(self, path):
        self.log.debug('[delete_file] Deleting {}'.format(path))
        owner, dataset_id, file_path = self._to_dw_path(path)
        if file_path is not None:
            self.api.delete_file(owner, dataset_id, file_path)
        else:
            raise DwError()  # TODO Proper error

    def is_hidden(self, path):
        self.log.debug('[is_hidden] Checking {}'.format(path))
        return False  # Nothing is hidden

    @property
    def root_path(self):
        return self.root_dir.strip('/')

    # noinspection PyMethodMayBeStatic
    def _checkpoints_class_default(self):
        return GenericFileCheckpoints

    # noinspection PyMethodMayBeStatic
    def _checkpoints_kwargs_default(self):
        kw = {
            'root_dir': tempfile.gettempdir()
        }
        return kw

    def _to_dw_path(self, path):
        if path == '':
            path = self.root_dir.strip('/')
        else:
            path = '{}/{}'.format(self.root_dir.strip('/'), path.strip('/'))

        path_parts = path.split('/', 2)

        owner = path_parts[0] if path_parts[0] != '' else None
        dataset_id = path_parts[1] if len(path_parts) > 1 else None
        file_path = path_parts[2] if len(path_parts) > 2 else None

        self.log.debug('[_to_dw_path] o:{} d:{} f:{}'.format(
            owner, dataset_id, file_path))

        return owner, dataset_id, file_path

    @staticmethod
    def _get_file(dataset, file_path):
        return next((f for f in dataset['files']
                     if f['name'] == file_path), None)

    @staticmethod
    def _split_parent(path):
        parent, _, name = path.rpartition('/')
        return parent, name


class DwError(Exception):
    # TODO Error handling "best practices"
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg
