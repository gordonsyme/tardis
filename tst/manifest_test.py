import os.path
import tempfile
import shutil
import functools
import textwrap
from contextlib import closing
from cStringIO import StringIO

from nose.tools import *
from mock import patch

from utilities import assert_really_equal, assert_really_not_equal

from tardis.util import sha1sum, makedirs
from tardis.tree import Tree
from tardis.manifest import StatInfo, FileEntry, DirectoryEntry, Manifest



##################
# StatInfo Tests #
##################
@raises(ValueError)
def test_stat_info_no_path():
    StatInfo.for_file(None)


###################
# FileEntry Tests #
###################

temp_dir = None
def setup_func():
    global temp_dir

    temp_dir = tempfile.mkdtemp(suffix="tardis_test")

    for i in range(10):
        filename = os.path.join(temp_dir, str(i))
        content = "This is content number {}".format(i)
        with open(filename, "wb") as f:
            f.write(content)


def teardown_func():
    global temp_dir
    shutil.rmtree(temp_dir)
    temp_dir = None


def expected_file_entry_for(i):
    filename = os.path.join(temp_dir, str(i))
    content = "This is content number {}".format(i)

    return FileEntry(filename, "data/{}/{}".format(sha1sum(str(i)), sha1sum(content)), StatInfo.for_file(filename))



@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_file_entry_no_path():
    file_path = os.path.join(temp_dir, '0')
    stat_info = StatInfo.for_file(file_path)
    FileEntry(None, "12345", stat_info)


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_file_entry_no_checksum():
    file_path = os.path.join(temp_dir, '0')
    stat_info = StatInfo.for_file(file_path)
    FileEntry(file_path, None, stat_info)


@with_setup(setup_func, teardown_func)
def test_file_entry_no_stat_info():
    file_path = os.path.join(temp_dir, '0')

    stat_info = StatInfo.for_file(file_path)
    expected = FileEntry(file_path, "12345", stat_info)

    assert_really_equal(expected, FileEntry(file_path, "12345"))


@with_setup(setup_func, teardown_func)
def test_file_entry_checksum():
    a = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    assert_equals("12345", a.checksum)


@with_setup(setup_func, teardown_func)
def test_file_entry_checksum():
    a = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    b = FileEntry(os.path.join(temp_dir, '0'), "data/54231/67890")
    assert_true(a.checksum_differs(b))
    assert_true(b.checksum_differs(a))


@with_setup(setup_func, teardown_func)
def test_file_entry_equality():
    a = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    b = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    c = FileEntry(os.path.join(temp_dir, '1'), "data/67890/09876")

    assert_really_equal(a, b)
    assert_really_not_equal(a, c)

    assert_equal(NotImplemented, a.__eq__(1))
    assert_equal(NotImplemented, a.__ne__(1))


@with_setup(setup_func, teardown_func)
def test_file_entry_as_fields():
    filename = os.path.join(temp_dir, '0')

    entry = FileEntry(filename, "data/{}/{}".format(sha1sum('0'), sha1sum("This is content number 0")))

    stat_info = StatInfo.for_file(filename)
    expected = (filename, "data/{}/{}".format(sha1sum('0'), sha1sum("This is content number 0"))) + stat_info

    assert_really_equal(expected, entry.as_fields())


@with_setup(setup_func, teardown_func)
def test_file_entry_from_fields_wrong_fields():
    filename = os.path.join(temp_dir, '0')

    with assert_raises(TypeError):
        fields = (filename, "12345", "data/object_id", 12345)
        FileEntry.from_fields(*fields)

    with assert_raises(TypeError):
        fields = (filename, "hi")
        FileEntry.from_fields(*fields)



#######################
# DirectoryEntry Tests#
#######################
@raises(ValueError)
@patch('os.path.isdir')
def test_directory_entry_for_directory_not_dir(isdir_mock):
    isdir_mock.return_value = False
    DirectoryEntry.for_directory(temp_dir)


@patch('os.path.isfile')
@with_setup(setup_func, teardown_func)
def test_directory_entry_for_directory_no_files(isfile_mock):
    isfile_mock.return_value = False

    expected = DirectoryEntry(temp_dir, [])
    assert_really_equal(expected, DirectoryEntry.for_directory(temp_dir))


@with_setup(setup_func, teardown_func)
def test_directory_entry_for_directory():
    expected_entries = [expected_file_entry_for(i) for i in range(10)]
    expected = DirectoryEntry(temp_dir, expected_entries)

    assert_really_equal(expected, DirectoryEntry.for_directory(temp_dir))


##################
# Manifest Tests #
##################
@with_setup(setup_func, teardown_func)
def test_manifest_equality():
    tree = Tree(DirectoryEntry.for_directory(temp_dir), [])
    expected_entries = { e.path: e for d in tree for e in d }
    expected_name = 'manifest/hostname/username/2013-03-18T15:33:50.122018'
    expected = Manifest(expected_name, expected_entries)

    with patch("tardis.manifest.iso8601") as iso8601:
        iso8601.return_value = '2013-03-18T15:33:50.122018'
        assert_really_equal(expected, Manifest.from_filesystem('hostname', 'username', [temp_dir]))

    with patch("tardis.manifest.iso8601") as iso8601:
        iso8601.return_value = '2013-03-24T00:00:00.000000'
        assert_really_not_equal(expected, Manifest.from_filesystem('hostname', 'username', [temp_dir]))

    assert_equal(NotImplemented, expected.__eq__(1))
    assert_equal(NotImplemented, expected.__ne__(1))


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_no_hostname():
    Manifest.from_filesystem(None, 'username', [temp_dir])


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_no_username():
    Manifest.from_filesystem('hostname', None, [temp_dir])


@raises(ValueError)
def test_manifest_from_filesystem_no_path():
    Manifest.from_filesystem('hostname', 'username', None)


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_path_is_not_directory():
    path = os.path.join(temp_dir, '0')
    Manifest.from_filesystem('hostname', 'username', [path])


@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem():
    tree = Tree(DirectoryEntry.for_directory(temp_dir), [])
    expected_entries = { e.path: e for d in tree for e in d }
    expected_name = 'manifest/hostname/username/2013-03-18T15:33:50.122018'
    expected = Manifest(expected_name, expected_entries)

    with patch("tardis.manifest.iso8601") as iso8601:
        iso8601.return_value = '2013-03-18T15:33:50.122018'

        assert_really_equal(expected, Manifest.from_filesystem('hostname', 'username', [temp_dir]))


@with_setup(setup_func, teardown_func)
def test_manifest_to_csv():
    def csv_row_for(i):
        filename = os.path.join(temp_dir, str(i))
        stat_info = StatInfo.for_file(filename)

        content = "This is content number {}".format(i)
        object_id = "data/{}/{}".format(sha1sum(str(i)), sha1sum(content))

        return "{}:{}:{}".format(filename, object_id, ":".join(str(field) for field in stat_info))

    manifest = Manifest.from_filesystem('hostname', 'username', [temp_dir])

    with closing(StringIO()) as csvfile:
        manifest.to_csv(csvfile)
        contents = csvfile.getvalue()

    expected = ['"{}"'.format(manifest._name)]
    expected += [csv_row_for(i) for i in range(10)]

    assert_equals(sorted(expected), sorted(contents.splitlines()))
