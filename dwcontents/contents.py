import base64
import json
import os
import tempfile

from notebook.services.contents.filecheckpoints import GenericFileCheckpoints
from notebook.services.contents.manager import ContentsManager
from tornado.web import HTTPError
from traitlets import Unicode

from dwcontents.api import DwContentsApi
from dwcontents.models import guess_type, DwMapper, guess_format
from dwcontents.utils import to_dw_path, split_parent, normalize_path, \
    directory_path


def http_400(msg):
    raise HTTPError(400, msg)


def http_403(msg):
    raise HTTPError(403, msg)


def http_404(msg):
    raise HTTPError(404, msg)


def http_409(msg):
    raise HTTPError(409, msg)


class DwContents(ContentsManager):
    dw_auth_token = Unicode(
        allow_none=False,
        config=True,
        help="data.world API authentication token.",
    )

    def __init__(self, **kwargs):
        super(DwContents, self).__init__(**kwargs)

        os.environ['DW_AUTH_TOKEN'] = self.dw_auth_token
        self.root_dir = normalize_path(self.root_dir)
        self.api = kwargs.get('api', DwContentsApi(self.dw_auth_token))
        self.mapper = DwMapper(root_dir=self.root_dir, logger=self.log)
        self.compatibility_mode = kwargs.get('compatibility_mode', False)

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
                for file in dataset.get('files', []):
                    file_dir, _ = split_parent(file['name'])
                    if file_dir.startswith(directory_path(dir_path)):
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
            type = guess_type(path, self.dir_exists)
            self.log.debug('[guess_type] Guessed {} type for {}'.format(
                type, path))

        if type == 'directory':
            if not self.dir_exists(path):
                raise http_404('No such entity')

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
                    dir_parent, dir_name = split_parent(file_path)
                    return self.mapper.map_subdir(
                        dir_name, dir_parent, dataset, include_content=content)
                else:
                    return self.mapper.map_dataset(
                        dataset, include_content=content)
        else:
            if file_path is None:
                http_400('Wrong type. {} is not a file.'.format(path))

            if not self.file_exists(path):
                http_404('No such entity')

            dir_parent, _ = split_parent(file_path)
            dataset = self.api.get_dataset(owner, dataset_id)
            file_obj = self._get_file(dataset, file_path)
            content_func = None
            if content:
                if type == 'notebook':
                    def content_func():
                        nb = self.api.get_file(
                            owner, dataset_id, file_path, 'json')
                        self.mark_trusted_cells(nb, path)
                        return nb
                else:
                    def content_func():
                        return self.api.get_file(
                            owner, dataset_id, file_path,
                            guess_format(file_path, type)
                            if format is None else format)

            model = self.mapper.map_file(
                file_obj, dir_parent, dataset,
                content_type=type,
                content_format=format,
                content_func=content_func)

            if content and model['type'] == 'notebook':
                self.validate_notebook_model(model)

            return model

    def rename_file(self, old_path, new_path):
        self.log.debug('[rename_file] Renaming {} to {}'.format(
            old_path, new_path))

        if old_path == '':  # TODO same for prefix
            http_409('Cannot rename root')

        if self.exists(new_path):
            http_409('File already exists ({})'.format(new_path))

        owner, dataset_id, file_path = self._to_dw_path(new_path)
        if file_path is None or self.dir_exists(old_path):
            if self.compatibility_mode:
                dataset = self.api.get_dataset(owner, dataset_id)
                for f in dataset.get('files', []):
                    parent = directory_path(old_path)
                    if f['name'].startswith(parent):
                        self.rename_file(
                            f['name'],
                            normalize_path(new_path, f['name'][len(parent):]))
            else:
                http_403('Only files can be renamed')

        old_file = self.get(old_path, content=True)
        self.save(old_file, new_path)
        self.delete_file(old_path)

    def save(self, model, path):
        self.log.debug('[save] Saving {} ({})'.format(path, model))
        self.run_pre_save_hook(model, path)

        owner, dataset_id, file_path = self._to_dw_path(path)
        file_dir, _ = split_parent(file_path)

        model_type = model['type']
        if model_type == 'directory':
            if self.compatibility_mode:
                self.api.upload_file(
                    owner, dataset_id,
                    normalize_path(file_path, 'dummy'), '')
                return self.mapper.map_subdir(
                    file_path, '', self.api.get_dataset(owner, dataset_id))
            else:
                http_403('Only files can be saved.')
        else:
            if file_path is None or self.dir_exists(path):
                http_400('Wrong type. {} is not a file.'.format(path))

            if model_type == 'notebook':
                self.check_and_sign(model['content'], path)
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
                content_type=model_type,
                content_format=model['format'])

    def delete_file(self, path):
        self.log.debug('[delete_file] Deleting {}'.format(path))
        owner, dataset_id, file_path = self._to_dw_path(path)
        if file_path is not None:
            if not self.exists(path):
                http_404('No such entity.')

            if guess_type(path, self.dir_exists) != 'directory':
                self.api.delete_file(owner, dataset_id, file_path)
            else:
                self.api.delete_directory(owner, dataset_id, file_path)
        else:
            http_403('Only dataset contents can be deleted.')

    def is_hidden(self, path):
        self.log.debug('[is_hidden] Checking {}'.format(path))
        return False  # Nothing is hidden

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
        self.log.debug('[_to_dw_path] p:{} r:{}'.format(path, self.root_dir))
        return to_dw_path(path, self.root_dir)

    @staticmethod
    def _get_file(dataset, file_path):
        file_path = normalize_path(file_path)
        return next((f for f in dataset.get('files', [])
                     if f['name'] == file_path), None)

