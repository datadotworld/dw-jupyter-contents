import base64
import json
import tempfile

from notebook.services.contents.filecheckpoints import GenericFileCheckpoints
from notebook.services.contents.manager import ContentsManager

from dwcontents.api import DwContentsApi
from dwcontents.models import map_root, map_account, map_dataset, map_subdir, \
    map_file, guess_type


class DwContents(ContentsManager):
    def __init__(self, *args, **kwargs):
        super(DwContents, self).__init__(*args, **kwargs)
        # TODO: Get token from config
        self.api = DwContentsApi(
            'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OnJmbHByciIsImlzcyI6ImFnZW50OnJmbHBycjo6OTZmM2VlMGMtNzUzMi00Zjc3LWI0OWQtNmU1ZDY5MDZhYWJjIiwiaWF0IjoxNDg2NTA3OTU1LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWV9.RxIZyvpi9K5zIRWoolgYq3U3c2mhvkc60wgVvAzaPbh7te6OgFCRYgvZiMuz-jQXAd9fO_2JgwHJbaWuPnaGUQ')
        # TODO: Deal with root dir separate from checkpoints root
        # TODO: Support hybrid config
        self.root_dir = tempfile.gettempdir()

    def dir_exists(self, path):
        self.log.debug('[dir_exists] Checking {}'.format(path))
        owner, dataset_id, dir_path = DwContents._to_dw_path(path)
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
                    file_dir, _ = DwContents._to_path_parts(file['name'])
                    if file_dir.startswith(dir_path):
                        return True
                return False
            else:
                return True

    def file_exists(self, path=''):
        self.log.debug('[file_exists] Checking {}'.format(path))
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        if owner is None or dataset_id is None:
            return False
        else:
            dataset = self.api.get_dataset(owner, dataset_id)
            if dataset is None:
                return False
            else:
                return file_path in [file['name'] for file in dataset['files']]

    def get(self, path, content=True, type=None, format=None):
        self.log.debug('[get] Getting {}/{}/{}/{}'.format(
            path, content, type, format))
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        if type is None or type == 'file':
            # TODO Why are notebooks marked as files?
            type, _, _ = guess_type(path, self.dir_exists)
            self.log.debug('[guess_type] Guessed {} type for {}'.format(
                type, path))

        if type == 'directory':
            if owner is None:
                # List root content
                return map_root(self.api.get_me(), self.api.get_datasets(),
                                include_content=True)
            elif dataset_id is None:
                # List account content
                return map_account(
                    owner,
                    [d for d in self.api.get_datasets()
                     if d['owner'] == owner],
                    include_content=True)
            else:
                # List dataset content
                dataset = self.api.get_dataset(owner, dataset_id)
                if file_path is not None:
                    dir_parent, dir_name = DwContents._to_path_parts(file_path)
                    return map_subdir(dir_parent, dir_name, dataset,
                                      include_content=True)
                else:
                    return map_dataset(dataset, include_content=True)
        else:
            dir_parent, _ = DwContents._to_path_parts(file_path)
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
            return map_file(
                file_obj, dir_parent, dataset,
                content_type=type,
                content_func=content_func)

    def rename_file(self, old_path, new_path):
        self.log.debug('[rename_file] Renaming {} to {}'.format(
            old_path, new_path))

        if self.dir_exists(old_path):
            raise DwError('Directory operations are not supported')

        old_file = self.get(old_path, content=True)
        self.save(old_file, new_path)
        self.delete_file(old_path)

    def save(self, model, path):
        self.log.debug('[save] Saving {} ({})'.format(path, model))
        self.run_pre_save_hook(model, path)
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        file_dir, _ = DwContents._to_path_parts(file_path)
        model_type = model['type']
        if model_type == 'directory':
            # TODO Allow project creation?
            # TODO Successful no-op subdir creation?
            raise DwError('Directory creation is not supported')
        else:
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

            return map_file(self._get_file(updated_dataset, file_path),
                            file_dir, updated_dataset,
                            content_type=model_type)

    def delete_file(self, path):
        self.log.debug('[delete_file] Deleting {}'.format(path))
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        if file_path is not None:
            self.api.delete_file(owner, dataset_id, file_path)
        elif dataset_id is not None:
            self.api.delete_dataset(owner, dataset_id)
        else:
            raise DwError('Not authorized to delete {}'.format(path))

    def is_hidden(self, path):
        self.log.debug('[is_hidden] Checking {}'.format(path))
        return False

    def _checkpoints_class_default(self):
        return GenericFileCheckpoints

    def _get_file(self, dataset, file_path):
        return next(f for f in dataset['files']
                    if f['name'] == file_path)

    @staticmethod
    def _to_dw_path(path):
        path_parts = path.strip('/').split('/', 2)

        owner = path_parts[0] if path_parts[0] != '' else None
        dataset_id = path_parts[1] if len(path_parts) > 1 else None
        file_path = path_parts[2] if len(path_parts) > 2 else None

        return owner, dataset_id, file_path

    @staticmethod
    def _to_path_parts(path):
        directory, _, name = path.rpartition('/')
        return directory, name


class DwError(Exception):
    # TODO Error handling "best practices"
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg