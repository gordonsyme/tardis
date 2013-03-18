import os.path
import tempfile
import shutil
from contextlib import closing
from cStringIO import StringIO

from nose.tools import *
from mock import patch

from utilities import assert_really_equal, assert_really_not_equal

from tardis.util import sha1sum
from tardis.manifest import StatInfo, FileEntry, DirectoryEntry, Manifest

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

    assert_equals(expected, FileEntry(file_path, "12345"))


@with_setup(setup_func, teardown_func)
def test_file_entry_equality():
    a = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    b = FileEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    c = FileEntry(os.path.join(temp_dir, '1'), "data/67890/09876")

    assert_really_equal(a, b)
    assert_really_not_equal(a, c)


@with_setup(setup_func, teardown_func)
def test_file_entry_as_fields():
    filename = os.path.join(temp_dir, '0')

    entry = FileEntry(filename, "data/{}/{}".format(sha1sum('0'), sha1sum("This is content number 0")))

    stat_info = StatInfo.for_file(filename)
    expected = (filename, "data/{}/{}".format(sha1sum('0'), sha1sum("This is content number 0"))) + stat_info

    assert_equals(expected, entry.as_fields())


@with_setup(setup_func, teardown_func)
def test_file_entry_from_fields_wrong_fields():
    filename = os.path.join(temp_dir, '0')

    with assert_raises(TypeError):
        fields = (filename, "12345", "data/object_id", 12345)
        FileEntry.from_fields(*fields)

    with assert_raises(TypeError):
        fields = (filename,)
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
    assert_equals(expected, DirectoryEntry.for_directory(temp_dir))


@with_setup(setup_func, teardown_func)
def test_directory_entry_for_directory():
    expected_entries = [expected_file_entry_for(i) for i in range(10)]
    expected = DirectoryEntry(temp_dir, expected_entries)

    assert_equals(expected, DirectoryEntry.for_directory(temp_dir))



##################
# Manifest Tests #
##################
@with_setup(setup_func, teardown_func)
def test_manifest_to_csv():
    def csv_row_for(i):
        filename = os.path.join(temp_dir, str(i))
        stat_info = StatInfo.for_file(filename)

        content = "This is content number {}".format(i)
        object_id = "data/{}/{}".format(sha1sum(str(i)), sha1sum(content))

        return "{}:{}:{}".format(filename, object_id, ":".join(str(field) for field in stat_info))

    manifest = Manifest.build_manifest('hostname', 'username', temp_dir)

    with closing(StringIO()) as csvfile:
        manifest.to_csv(csvfile)
        contents = csvfile.getvalue()

    expected = ['"{}"'.format(manifest._name)]
    expected += [csv_row_for(i) for i in range(10)]

    assert_equals(sorted(expected), sorted(contents.splitlines()))
