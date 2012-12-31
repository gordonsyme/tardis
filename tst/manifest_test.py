import os.path
import tempfile
import shutil
from contextlib import closing
from cStringIO import StringIO

from nose.tools import *
from mock import patch

from utilities import assert_really_equal, assert_really_not_equal

from tardis.util import sha1sum
from tardis.manifest import ManifestEntry, Manifest

#######################
# ManifestEntry Tests #
#######################

temp_dir = None
def setup_func():
    global temp_dir
    global temp_dir_content_sum

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


def expected_manifest_for(i):
    filename = os.path.join(temp_dir, str(i))
    content = "This is content number {}".format(i)

    return ManifestEntry(filename, "data/{}/{}".format(sha1sum(str(i)), sha1sum(content)))



@raises(ValueError)
def test_manifest_entry_no_path():
    ManifestEntry(None, "12345")


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_entry_no_checksum():
    ManifestEntry(temp_dir, None)


@with_setup(setup_func, teardown_func)
def test_manifest_entry_equality():
    a = ManifestEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    b = ManifestEntry(os.path.join(temp_dir, '0'), "data/54231/12345")
    c = ManifestEntry(os.path.join(temp_dir, '0'))

    assert_really_equal(a, b)
    assert_really_not_equal(a, c)


@raises(ValueError)
@patch('os.path.isdir')
def test_manifest_entry_for_directory_not_dir(isdir_mock):
    isdir_mock.return_value = False
    ManifestEntry.for_directory(temp_dir)


@raises(ValueError)
@patch('os.path.isfile')
@with_setup(setup_func, teardown_func)
def test_manifest_entry_for_directory_no_files(isfile_mock):
    isfile_mock.return_value = False
    ManifestEntry.for_directory(temp_dir)


@with_setup(setup_func, teardown_func)
def test_manifest_entry_for_directory():
    entries = sorted( ManifestEntry.for_directory(temp_dir) )

    expected = [expected_manifest_for(i) for i in range(10)]

    assert_equals(expected, entries)


@with_setup(setup_func, teardown_func)
def test_manifest_entry_as_fields():
    filename = os.path.join(temp_dir, '0')

    entry = ManifestEntry(filename)
    expected = (filename, "data/{}/{}".format(sha1sum('0'), sha1sum("This is content number 0")))
    assert_equals(expected, entry.as_fields())


@with_setup(setup_func, teardown_func)
def test_manifest_entry_from_fields_wrong_fields():
    filename = os.path.join(temp_dir, '0')

    with assert_raises(TypeError):
        fields = (filename, "12345", "data/object_id", 12345)
        ManifestEntry.from_fields(*fields)

    with assert_raises(TypeError):
        fields = (filename,)
        ManifestEntry.from_fields(*fields)



##################
# Manifest Tests #
##################
@with_setup(setup_func, teardown_func)
def test_manifest_to_csv():
    def csv_row_for(i):
        filename = os.path.join(temp_dir, str(i))
        content = "This is content number {}".format(i)

        return "{}:{}".format(filename, "data/{}/{}".format(sha1sum(str(i)), sha1sum(content)))

    manifest = Manifest.for_directories('hostname', 'username', [temp_dir])

    with closing(StringIO()) as csvfile:
        manifest.to_csv(csvfile)
        contents = csvfile.getvalue()

    expected = ['"{}"'.format(manifest.name)]
    expected += [csv_row_for(i) for i in range(10)]

    assert_equals(sorted(expected), sorted(contents.splitlines()))
