from notebook.services.contents.manager import ContentsManager

from dwcontents.api import DwContentsApi


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
            root = DwContents._map_root(self.api.get_me())
            accounts = sorted({d['owner'] for d in self.api.get_datasets()})
            root['content'] = [DwContents._map_account(
                self.api.get_user(a)) for a in accounts]
            return root
        elif dataset_id is None:
            # List account content
            self.log.info('Processing {}'.format(owner))
            account_dir = DwContents._map_account(self.api.get_user(owner))
            account_dir['content'] = [DwContents._map_dataset(d)
                                      for d in self.api.get_datasets()
                                      if d['owner'] == owner]
            return account_dir
        else:
            if type == 'directory':
                # List dataset content
                self.log.info('Processing {}'.format(dataset_id))
                dataset = self.api.get_dataset(owner, dataset_id)

                prefix = file_path if file_path is not None else ''
                directory_files = [f for f in dataset['files']
                                   if f['name'].startswith(prefix)]

                files = []
                subdirectories = dict()
                for f in directory_files:
                    if '/' not in f['name']:
                        files.append({
                            'name': f['name'],
                            'path': '{}/{}/{}/{}'.format(
                                owner, dataset_id, prefix, f['name']
                            ),
                            'created': f['created'],
                            'last_modified': f['updated']
                        })
                    else:
                        subdir_name = f['name'].split('/')[0]
                        subdir = subdirectories.get(subdir_name, dict())
                        subdir.update({
                            'name': subdir_name,
                            'path': '{}/{}/{}/{}'.format(
                                owner, dataset_id, prefix, subdir_name
                            ),
                            'created': (
                                f['created']
                                if ('created' not in subdir
                                    or subdir['created'] > f['created'])
                                else subdir['created']),
                            'last_modified': (
                                f['updated']
                                if ('last_modified' not in subdir
                                    or subdir['last_modified'] < f['updated'])
                                else subdir['last_modified'])
                        })
                        subdirectories[subdir_name] = subdir

                files = sorted(files, key=lambda f: f['name'])
                subdir_list = sorted(subdirectories.values(),
                                     key=lambda s: s['name'])

                dataset_dir = DwContents._map_dataset(dataset)
                dataset_dir['content'] = (
                    [DwContents._map_subdirectory(s)
                     for s in subdir_list] +
                    [DwContents._map_content_free_entity(f)
                     for f in files])

                return dataset_dir

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
    def _map_root(me):
        return DwContents._map_directory('', '', me['created'], me['updated'],
                                         False)

    @staticmethod
    def _map_account(user):
        return DwContents._map_directory(
            user['id'], user['id'], user['created'], user['updated'], False)

    @staticmethod
    def _map_dataset(dataset):
        return DwContents._map_directory(
            dataset['title'],
            '{}/{}'.format(dataset['owner'], dataset['id']),
            dataset['created'],
            dataset['updated']
        )

    @staticmethod
    def _map_subdirectory(subdir):
        return DwContents._map_directory(
            subdir['name'],
            subdir['path'],
            subdir['created'],
            subdir['last_modified'],
            False
        )

    @staticmethod
    def _map_content_free_entity(file):
        return {
            'name': file['name'],
            'path': file['path'],
            'type': 'notebook' if file['name'].endswith('ipynb') else 'file',
            'created': file['created'],
            'last_modified': file['last_modified'],
            'mimetype': None if file['name'].endswith(
                'ipynb') else 'application/octet-stream',
            'writable': True,
            'format': 'json' if file['name'].endswith('ipynb') else 'base64'
        }

    @staticmethod
    def _map_directory(name, path, created, last_modified, writable=True):
        return {
            'name': name,
            'path': path,
            'type': 'directory',
            'created': created,
            'last_modified': last_modified,
            'mimetype': None,
            'writable': writable,
            'format': 'json'
        }

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
