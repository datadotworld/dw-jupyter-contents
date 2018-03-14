from doublex import assert_that
from hamcrest import equal_to

from dwcontents.utils import to_dw_path, relative_path, split_parent, \
    to_api_path, normalize_path, unique_justseen, directory_path


def test_directory_path():
    assert_that(directory_path(''), equal_to(''))
    assert_that(directory_path('a'), equal_to('a/'))
    assert_that(directory_path('a/'), equal_to('a/'))


def test_normalize_path():
    assert_that(normalize_path(''), equal_to(''))
    assert_that(normalize_path('/'), equal_to(''))
    assert_that(normalize_path('', ''), equal_to(''))
    assert_that(normalize_path('path'), equal_to('path'))
    assert_that(normalize_path('/path'), equal_to('path'))
    assert_that(normalize_path('path/path/'), equal_to('path/path'))
    assert_that(normalize_path('path/', '/path'), equal_to('path/path'))


def test_relative_path():
    assert_that(relative_path('a/b', 'a'), equal_to('b'))
    assert_that(relative_path('a', 'a'), equal_to(''))
    assert_that(relative_path('a/b', ''), equal_to('a/b'))


def test_split_parent():
    assert_that(split_parent(''), equal_to(('', '')))
    assert_that(split_parent('a'), equal_to(('', 'a')))
    assert_that(split_parent('a/b'), equal_to(('a/', 'b')))
    assert_that(split_parent('a/b/c'), equal_to(('a/b/', 'c')))


def test_to_api_path():
    assert_that(to_api_path('owner'), equal_to('owner'))
    assert_that(to_api_path('owner/dataset'), equal_to('owner/dataset'))
    assert_that(to_api_path('owner/dataset/file.ext'),
                equal_to('owner/dataset/file.ext'))
    assert_that(to_api_path('owner/dataset/file.ext', root_dir='owner'),
                equal_to('dataset/file.ext'))
    assert_that(to_api_path('owner/dataset/file.ext',
                            root_dir='owner', prefix='dw'),
                equal_to('dw/dataset/file.ext'))


def test_to_dw_path():
    assert_that(to_dw_path('owner'),
                equal_to(('owner', None, None)))
    assert_that(to_dw_path('owner/dataset'),
                equal_to(('owner', 'dataset', None)))
    assert_that(to_dw_path('owner/dataset/file.ext'),
                equal_to(('owner', 'dataset', 'file.ext')))
    assert_that(to_dw_path('owner/dataset/subdir/file.ext'),
                equal_to(('owner', 'dataset', 'subdir/file.ext')))
    assert_that(to_dw_path('dataset/file.ext', root_dir='owner'),
                equal_to(('owner', 'dataset', 'file.ext')))
    assert_that(to_dw_path('dw/dataset/file.ext',
                           root_dir='owner', prefix='dw'),
                equal_to(('owner', 'dataset', 'file.ext')))


def test_unique_justseen():
    objs = [{'name': 'bbb'}, {'name': 'aaa'}, {'name': 'bbb'}]
    assert_that(list(unique_justseen(objs, lambda o: o['name'])),
                equal_to([{'name': 'aaa'}, {'name': 'bbb'}]))


