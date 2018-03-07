from dwcontents.contents import DwContents


def test_root():
    root_dir = DwContents().get(
        path='', content=False, type='directory', format='json')
    print(root_dir)
