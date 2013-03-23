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
@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_no_hostname():
    Manifest.from_filesystem(None, 'username', temp_dir)


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_no_username():
    Manifest.from_filesystem('hostname', None, temp_dir)


@raises(ValueError)
def test_manifest_from_filesystem_no_path():
    Manifest.from_filesystem('hostname', 'username', None)


@raises(ValueError)
@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem_path_is_not_directory():
    path = os.path.join(temp_dir, '0')
    Manifest.from_filesystem('hostname', 'username', path)


@with_setup(setup_func, teardown_func)
def test_manifest_from_filesystem():
    tree = Tree(DirectoryEntry.for_directory(temp_dir), [])
    expected_entries = { e.path: e for d in tree for e in d }
    expected_name = 'manifest/hostname/username/2013-03-18T15:33:50.122018'
    expected = Manifest(expected_name, expected_entries)

    with patch("tardis.manifest.iso8601") as iso8601:
        iso8601.return_value = '2013-03-18T15:33:50.122018'

        assert_really_equal(expected, Manifest.from_filesystem('hostname', 'username', temp_dir))


@with_setup(setup_func, teardown_func)
def test_manifest_to_csv():
    def csv_row_for(i):
        filename = os.path.join(temp_dir, str(i))
        stat_info = StatInfo.for_file(filename)

        content = "This is content number {}".format(i)
        object_id = "data/{}/{}".format(sha1sum(str(i)), sha1sum(content))

        return "{}:{}:{}".format(filename, object_id, ":".join(str(field) for field in stat_info))

    manifest = Manifest.from_filesystem('hostname', 'username', temp_dir)

    with closing(StringIO()) as csvfile:
        manifest.to_csv(csvfile)
        contents = csvfile.getvalue()

    expected = ['"{}"'.format(manifest._name)]
    expected += [csv_row_for(i) for i in range(10)]

    assert_equals(sorted(expected), sorted(contents.splitlines()))


@with_setup(setup_func, teardown_func)
def test_manifest_from_cache():
    def write_file(name, contents):
        with open(name, 'w') as f:
            f.write(contents)

    temp_dir = tempfile.mkdtemp(suffix="tardis_test")
    makedirs(os.path.join(temp_dir, 'foo', 'bar'))
    makedirs(os.path.join(temp_dir, 'baz'))

    path_join = functools.partial(os.path.join, temp_dir)

# FIXME Shouldn't check in commented out code - this is necessary to build the
# actual directory structure and files that are named by the .tardis_manifest
# cached values below.
#    # Set up the files and contents
#    file_contents = { path_join('foo', 'file1'): "The Quick Brown Fox",
#                      path_join('foo', 'file2'): "Jumps Over The Lazy Dog",
#                      path_join('foo', 'bar', 'file3'): "How now",
#                      path_join('foo', 'bar', 'file4'): "Brown cow",
#                      path_join('foo', 'bar', 'file5'): "Hmmm...",
#                      path_join('baz', 'file1'): "Hello world"
#                    }
#
#    for name, contents in file_contents:
#        write_file(name, contents)

    # Set up the manifest caches
    cache_name = '.tardis_manifest'

    cache_contents = {
            path_join(cache_name): "",
            path_join('foo', cache_name): textwrap.dedent("""
                                    {directory}/foo/file1:data/60b27f004e454aca81b0480209cce5081ec52390/ad9ea5b229df4b040d6df93cc2a5f3629dcf19b1:username:groupname:436:1363623087:1363623087:20
                                    {directory}/foo/file2:data/cb99b709a1978bd205ab9dfd4c5aaa1fc91c7523/8958990b773b952af60ccca72b89ec494f3cf961:username:groupname:436:1363623102:1363623102:24
                                    """).strip().format(directory=temp_dir),
            path_join('foo', 'bar', cache_name): textwrap.dedent("""
                                    {directory}/foo/bar/file3:data/d5b0a58bc47161b1b8a831084b366f757c4f0b11/6d4403f7e18e4446d5255f3e16fbc5f4c3dbadd7:username:groupname:436:1363623135:1363623135:8
                                    {directory}/foo/bar/file4:data/1b641bf4f6b84efcd42920ff1a88ff2f97fb9d08/d1f308de70983ba84f040e1213f357135dd6862d:username:groupname:436:1363623143:1363623143:10
                                    {directory}/foo/bar/file5:data/c1750bee9c1f7b5dd6f025b645ab6eba5df94175/a4b5fced08c0e9e4132fbe467320669a70cb9043:username:groupname:436:1363623150:1363623150:8
                                    """).strip().format(directory=temp_dir),
            path_join('baz', cache_name): textwrap.dedent("""
                                    {directory}/baz/file1:data/60b27f004e454aca81b0480209cce5081ec52390/33ab5639bfd8e7b95eb1d8d0b87781d4ffea4d5d:username:groupname:436:1363623181:1363623181:12
                                    """).strip().format(directory=temp_dir),
        }

    for name, contents in cache_contents.iteritems():
        write_file(name, contents)

    expected_entries = { path_join('foo', 'file1'):  FileEntry( path_join('foo', 'file1')
                                                              , "data/60b27f004e454aca81b0480209cce5081ec52390/ad9ea5b229df4b040d6df93cc2a5f3629dcf19b1"
                                                              , StatInfo("username" , "groupname" , 436 , 1363623087 , 1363623087 , 20)
                                                              )
                       , path_join('foo', 'file2'): FileEntry( path_join('foo', 'file2')
                                                             , "data/cb99b709a1978bd205ab9dfd4c5aaa1fc91c7523/8958990b773b952af60ccca72b89ec494f3cf961"
                                                             , StatInfo("username" , "groupname" , 436 , 1363623102 , 1363623102 , 24)
                                                             )
                       , path_join('foo', 'bar', 'file3'): FileEntry( path_join('foo', 'bar', 'file3')
                                                                    , "data/d5b0a58bc47161b1b8a831084b366f757c4f0b11/6d4403f7e18e4446d5255f3e16fbc5f4c3dbadd7"
                                                                    , StatInfo("username" , "groupname" , 436 , 1363623135 , 1363623135 , 8)
                                                                    )
                       , path_join('foo', 'bar', 'file4'): FileEntry( path_join('foo', 'bar', 'file4')
                                                                    , "data/1b641bf4f6b84efcd42920ff1a88ff2f97fb9d08/d1f308de70983ba84f040e1213f357135dd6862d"
                                                                    , StatInfo("username" , "groupname" , 436 , 1363623143 , 1363623143 , 10)
                                                                    )
                       , path_join('foo', 'bar', 'file5'): FileEntry( path_join('foo', 'bar', 'file5')
                                                                    , "data/c1750bee9c1f7b5dd6f025b645ab6eba5df94175/a4b5fced08c0e9e4132fbe467320669a70cb9043"
                                                                    , StatInfo("username" , "groupname" , 436 , 1363623150 , 1363623150 , 8)
                                                                    )
                       , path_join('baz', 'file1'): FileEntry( path_join('baz', 'file1')
                                                             , "data/60b27f004e454aca81b0480209cce5081ec52390/33ab5639bfd8e7b95eb1d8d0b87781d4ffea4d5d"
                                                             , StatInfo("username" , "groupname" , 436 , 1363623181 , 1363623181 , 12)
                                                             )
                       }

    expected_name = 'manifest/hostname/username/2013-03-18T15:33:50.122018'
    expected = Manifest(expected_name, expected_entries)

    with patch("tardis.manifest.iso8601") as iso8601:
        iso8601.return_value = '2013-03-18T15:33:50.122018'

        manifest = Manifest.from_cache('hostname', 'username', temp_dir)

        assert_really_equal(expected, manifest)
