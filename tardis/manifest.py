import os
import os.path
import pwd, grp, stat

import logging
import csv
import itertools
import functools
from collections import namedtuple

from tardis.util import sha1sum, iso8601
from tardis.tree import Tree


class StatInfo(namedtuple('StatInfo', [ 'owner' , 'group' , 'mode' , 'ctime' , 'mtime' , 'size' ])):
    __slots__ = () # avoid creating an instance __dict__

    @classmethod
    def for_file(cls, path):
        if not path:
            raise ValueError("Must specify a file path")

        stat_struct = os.stat(path)

        owner = pwd.getpwuid(stat_struct.st_uid).pw_name
        group = grp.getgrgid(stat_struct.st_gid).gr_name
        mode  = stat.S_IMODE(stat_struct.st_mode)

        ctime = os.path.getctime(path)
        mtime = os.path.getmtime(path)
        size  = os.path.getsize(path)

        return cls(owner, group, mode, int(ctime), int(mtime), size)



class FileEntry(object):
    """An entry in the backup manifest

    FileEntry objects maintain the relationship between a file name, the
    sha1sum of its content and the S3 object ID naming the content.

    TODO insert comment about stat info

    Note that the content may not exist in S3 yet.
    """
    def __init__(self, file_path, object_id, stat_info=None):
        """Create a FileEntry.

        file_path - file name
        checksum - checksum of the file's content.
        object_id - S3 object id for the content tarball.
        stat_info - a StatInfo instance for this file, will be calculated from the file if not specified.
        """
        logging.debug("Creating file entry for {}".format(file_path))

        if not file_path:
            raise ValueError("Must specify a file path")

        if not object_id:
            raise ValueError("Must specify an object id")

        if not stat_info:
            stat_info = StatInfo.for_file(file_path)

        self.path = file_path
        self.object_id = object_id
        self.stat_info = stat_info

    def __repr__(self):
        return "<FileEntry({!r}, {!r}, {!r})>".format(self.path, self.object_id, self.stat_info)

    @property
    def checksum(self):
        return os.path.basename(self.object_id)

    def checksum_differs(self, other):
        return self.checksum != other.checksum

    @classmethod
    def from_fields(cls, fields):
        logging.debug("Creating FileEntry from: {}".format(fields))
        return cls(*fields)

    def as_fields(self):
        return (self.path, self.object_id) + self.stat_info

    def __eq__(self, other):
        if isinstance(other, FileEntry):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result


class DirectoryEntry(object):
    """
    """
    def __init__(self, path, entries=None):
        self.path = path
        self.entries = entries

    def __getitem__(self, item):
        return self.entries.__getitem__(item)

    def __contains__(self, item):
        return self.entries.__contains__(item)

    def __iter__(self):
        return self.entries.__iter__()

    def __eq__(self, other):
        if isinstance(other, DirectoryEntry):
            return all([self.path == other.path, self.entries == other.entries])
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __repr__(self):
        return "<DirectoryEntry({!r}, {!r})>".format(self.path, self.entries)

    def write_cache(self):
        """Write this directory entry to an on-disk cache"""
        # I don't like this being here, feels impure and out-of-place
        with open(os.path.join(self.path, '.tardis_manifest'), 'w') as f:
            writer = csv.writer(f, delimiter=':', lineterminator='\n')
            writer.writerows(entry.as_fields() for entry in self.entries)

    @classmethod
    def from_cache(cls, path):
        """Read this directory entry from an on-disk cache"""
        # I don't like this being here, feels impure and out-of-place
        def to_entry(row):
            stat_info = StatInfo(row[2], row[3], int(row[4]), int(row[5]), int(row[6]), long(row[7]))
            return FileEntry(row[0], row[1], stat_info)

        with open(os.path.join(path, '.tardis_manifest')) as f:
            reader = csv.reader(f, delimiter=':', lineterminator='\n')
            entries = [to_entry(row) for row in reader]

        return cls(path, entries)


    @classmethod
    def for_directory(cls, path):
        if not os.path.isdir(path):
            raise ValueError("{} does not name a directory".format(path))

        path = os.path.abspath(path)

        logging.debug("Creating manifest for {}".format(path))

        paths = [os.path.join(path, e) for e in sorted(os.listdir(path))]

        entries = [FileEntry(f, cls._object_id(f), StatInfo.for_file(f))
                       for f in paths
                       if os.path.isfile(f)]

        return DirectoryEntry(path, entries)

    @classmethod
    def _object_id(cls, path):
        checksum = cls._checksum_for_file(path)
        return "data/{}/{}".format(sha1sum(os.path.basename(path)), checksum)

    @classmethod
    def _checksum_for_file(cls, path):
        if not os.path.isfile(path):
            raise ValueError("{} does not name a file".format(path))

        with open(path, 'rb') as f:
            parts = iter(functools.partial(f.read, 32768), '')
            checksum = sha1sum(parts)

            logging.debug("{} --> {}".format(path, checksum))

        logging.debug("Content checksum: {}".format(checksum))

        return checksum



class Manifest(object):
    """A backup manifest.

    <
    Recursive structure:
        - current directory
        - ManifestEntries for each regular file
        - Manifests for each subdirectory
    >

    Maps file paths to the FileEntry for that file.

    Manifest keys are named in the following format:

    manifest/{hostname}/{user}/{timestamp}
    """
    def __init__(self, name, manifest_tree):
        self._name = name
        self._manifest_tree = manifest_tree

    def __iter__(self):
        return itertools.chain(self._manifest_tree)

    def __eq__(self, other):
        if isinstance(other, Manifest):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def to_csv(self, stream):
        # Write the name of *this* manifest, but not the sub-manifests
        # Each entry needs to know the path in order to properly write filenames
        logging.debug("Writing {} to csv, entries: {}".format(self.__class__, self._manifest_tree))

        writer = csv.writer(stream, delimiter=':', lineterminator='\n')
        writer.writerow([self._name])
        for directory_entry in self._manifest_tree:
            writer.writerows(entry.as_fields() for entry in directory_entry)

    @classmethod
    def from_filesystem(cls, hostname, user, path, ignored_directories=None):
        to_directory_entry = functools.partial(DirectoryEntry.for_directory)

        return cls._build_manifest(hostname, user, path, to_directory_entry, ignored_directories)

    @classmethod
    def _build_manifest(cls, hostname, user, path, f, ignored_directories=None):
        if not hostname:
            raise ValueError("hostname must be a non-empty string")

        if not user:
            raise ValueError("user must be a non-empty string")

        if not path:
            raise ValueError("path must be a non-empty string")

        if not f:
            raise ValueError("transform function must be specified")

        path = os.path.abspath(path)
        if not os.path.isdir(path):
            raise ValueError("{} does not name a directory".format(path))

        if not ignored_directories:
            ignored_directories = []

        def child_directories(dirname):
            names = [os.path.join(dirname, name) for name in os.listdir(dirname) if not name.startswith(".")]
            return [name for name in names if os.path.isdir(name) and not name in ignored_directories]

        directory_tree = Tree.build_tree(path, child_directories)

        manifest_tree = directory_tree.fmap(f)

        return Manifest(cls.name_for(hostname, user), manifest_tree)

    @classmethod
    def name_prefix_for(cls, hostname, user):
        return "manifest/{host}/{user}/".format(host=hostname, user=user)

    @classmethod
    def name_for(cls, hostname, user):
        return cls.name_prefix_for(hostname, user) + iso8601()

    # In each directory write:
    # .tardis_manifest which contains just the *entries* for that directory
    #
    # To re-create a manifest:
    # read .tardis_manifest -> gets the manifest entries
    # create a manifest for each sub-directory (either by reading .tardis_manifest or creating from scratch)
    def write_cache(self):
        for directory_entry in self._manifest_tree:
            directory_entry.write_cache()

    @classmethod
    def from_cache(cls, hostname, user, path, ignored_directories=None):
        directory_entry_from_cache = DirectoryEntry.from_cache

        return cls._build_manifest(hostname, user, path, directory_entry_from_cache, ignored_directories)
