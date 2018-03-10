from notebook.services.contents.filemanager import FileContentsManager
from notebook.services.contents.tests.test_manager import TestContentsManager

from dwcontents.contents import DwContents
import nbformat


def test_get_notebook():
    cm = DwContents()
    nb = cm.get('rflprr/daily-l-28-cdf/L28 CDF.ipynb',
                True, 'notebook', 'json')
    cp = cm.create_checkpoint('rflprr/daily-l-28-cdf/L28 CDF.ipynb')
    valid = nbformat.validate(nb)
    FileContentsManager().save(nb, 'test-contents.ipynb')
    pass


class DwContentsTest(TestContentsManager):
    def setUp(self):
        self.contents_manager = DwContents()

    def tearDown(self):
        pass
