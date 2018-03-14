from notebook.services.contents.tests.test_manager import TestContentsManager
from pytest import mark

from dwcontents.contents import DwContents
from dwcontents.utils import normalize_path


@mark.usefixtures('api_class')
class DwContentsTest(TestContentsManager):

    def setUp(self):
        self.api = self.api_class()
        self.contents_manager = DwContents(
            root_dir='testy-tester/jupyter',
            compatibility_mode=True,
            api=self.api
        )

    def tearDown(self):
        pass

    def make_dir(self, api_path):
        dummy_file = normalize_path(api_path, 'dummy')
        self.api.upload_file('testy-tester', 'jupyter', dummy_file, '')
