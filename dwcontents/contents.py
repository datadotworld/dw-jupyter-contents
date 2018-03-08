from notebook.services.contents.manager import ContentsManager

from dwcontents.api import DwContentsApi
from dwcontents.models import map_root, map_account, map_dataset, map_subdir


class DwContents(ContentsManager):
    def __init__(self, *args, **kwargs):
        super(DwContents, self).__init__(*args, **kwargs)
        # TODO: Get token from config
        self.api = DwContentsApi(
            'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OnJmbHByciIsImlzcyI6ImFnZW50OnJmbHBycjo6OTZmM2VlMGMtNzUzMi00Zjc3LWI0OWQtNmU1ZDY5MDZhYWJjIiwiaWF0IjoxNDg2NTA3OTU1LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWV9.RxIZyvpi9K5zIRWoolgYq3U3c2mhvkc60wgVvAzaPbh7te6OgFCRYgvZiMuz-jQXAd9fO_2JgwHJbaWuPnaGUQ')

    def dir_exists(self, path):
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
                    file_dir, _, _ = file['name'].rpartition('/')
                    if file_dir.startswith(dir_path):
                        return True
                return False
            else:
                return True

    def file_exists(self, path=''):
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
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        self.log.info('Processing request for {}'.format(path))
        if owner is None:
            # List root content
            self.log.info('Processing root')
            return map_root(self.api.get_me(), self.api.get_datasets(),
                            include_content=True)
        elif dataset_id is None:
            # List account content
            self.log.info('Processing account {}'.format(owner))
            return map_account(
                owner,
                [d for d in self.api.get_datasets() if d['owner'] == owner],
                include_content=True)
        else:
            if type == 'directory':
                # List dataset content
                dataset = self.api.get_dataset(owner, dataset_id)
                if file_path is not None:
                    parent, _, subdir = file_path.rpartition('/')
                    self.log.info('Processing subdir {} of {}'.format(
                        subdir, parent
                    ))
                    return map_subdir(subdir, parent, dataset,
                                      include_content=True)
                else:
                    self.log.info('Processing dataset {}'.format(dataset_id))
                    return map_dataset(dataset, include_content=True)
            else:
                pass

    def rename_file(self, old_path, new_path):
        pass

    def save(self, model, path):
        pass

    def delete_file(self, path):
        owner, dataset_id, file_path = DwContents._to_dw_path(path)
        if file_path is not None:
            self.api.delete_file(owner, dataset_id, file_path)
        elif dataset_id is not None:
            self.api.delete_dataset(owner, dataset_id)
        else:
            raise DwError('Not authorized to delete {}'.format(path))

    def is_hidden(self, path):
        return False

    @staticmethod
    def _to_dw_path(path):
        path_parts = path.strip('/').split('/', 2)

        owner = path_parts[0] if path_parts[0] != '' else None
        dataset_id = path_parts[1] if len(path_parts) > 1 else None
        file_path = path_parts[2] if len(path_parts) > 2 else None

        return owner, dataset_id, file_path


class DwError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg
