import os
import os.path
import pwd, grp, stat

import logging
import csv
import itertools
import functools
from collections import namedtuple, defaultdict

from tardis.util import sha1sum, iso8601
from tardis.tree import Tree


class StatInfo(namedtuple('StatInfo', [ 'owner' , 'group' , 'mode' , 'ctime' , 'mtime' , 'size' ])):
    __slots__ = () # avoid creating an instance __dict__

    def apply_to(self, path):
        if not path or not os.path.isfile(path):
            raise ValueError("Must specify a file path")

        uid = pwd.getpwdnam(self.owner).pw_uid
        gid = grp.getgrnam(self.group).gr_gid
        os.chown(path, uid, gid)

        os.chmod(path, self.mode)

        os.utime(path, (self.mtime, self.mtime))

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
        stat_info = StatInfo(fields[2], fields[3], int(fields[4]), int(fields[5]), int(fields[6]), long(fields[7]))
        return cls(fields[0], fields[1], stat_info)

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


class NullFileEntry(object):
    def __init__(self):
        self.stat_info = tuple()

    def checksum_differs(self, other):
        return True

    def checksum(self):
        return "no checksum"


class DirectoryEntry(object):
    """
    """
    _CACHE_IGNORE_LIMIT = 2**16

    def __init__(self, path, entries=None):
        self.path = path
        self.entries = entries

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
        with open(os.path.join(path, '.tardis_manifest')) as f:
            reader = csv.reader(f, delimiter=':', lineterminator='\n')
            entries = [FileEntry.from_fields(row) for row in reader]

        return cls(path, entries)

    @classmethod
    def for_directory(cls, path):
        if not os.path.isdir(path):
            raise ValueError("{} does not name a directory".format(path))

        def create_cache(cache_path):
            try:
                with open(os.path.join(cache_path, '.tardis_manifest')) as f:
                    reader = csv.reader(f, delimiter=':', lineterminator='\n')
                    entries = (FileEntry.from_fields(row) for row in reader)

                    return { entry.path: entry for entry in entries }
            except (OSError, IOError):
                logging.debug("Unable to create cache", exc_info=True)
                return {}

        def get_file_entry(cache, file_path):
            stat_info = StatInfo.for_file(file_path)

            if stat_info.size > cls._CACHE_IGNORE_LIMIT:
                cached_entry = cache.get(file_path, NullFileEntry())
                if cached_entry.stat_info == stat_info:
                    logging.debug("Using cached data for {}".format(file_path))
                    return cached_entry
            return FileEntry(file_path, cls._object_id(file_path), stat_info)

        path = os.path.abspath(path)

        logging.debug("Creating directory entry for {}".format(path))

        make_entry = functools.partial(get_file_entry, create_cache(path))

        paths = [os.path.join(path, e) for e in sorted(os.listdir(path)) if e != '.tardis_manifest']
        entries = [make_entry(f) for f in paths if os.path.isfile(f)]

        return cls(path, entries)

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

    Maps file paths to the FileEntry for that file.

    Manifest keys are named in the following format:

    manifest/{hostname}/{user}/{timestamp}
    """
    def __init__(self, name, manifest):
        self._name = name
        self._manifest = dict(manifest)

    def __iter__(self):
        return iter(self._manifest)

    def __getitem__(self, item):
        return self._manifest.get(item, NullFileEntry())

    def __contains__(self, item):
        return self._manifest.__contains__(item)

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
        logging.debug("Writing {} to csv, entries: {}".format(self.__class__, self._manifest))

        writer = csv.writer(stream, delimiter=':', lineterminator='\n')
        writer.writerow([self._name])
        for path in self._manifest:
            entry = self._manifest[path]
            writer.writerow(entry.as_fields())

    @classmethod
    def from_csv(cls, stream):
        reader = csv.reader(stream, delimiter=':', lineterminator='\n')
        manifest_name = next(reader)[0]

        file_entries = dict()
        for row in reader:
            entry = FileEntry.from_fields(row)
            file_entries[entry.path] = entry

        return cls(manifest_name, file_entries)

    @classmethod
    def from_filesystem(cls, hostname, user, paths, ignored_directories=None):
        if not hostname:
            raise ValueError("hostname must be a non-empty string")

        if not user:
            raise ValueError("user must be a non-empty string")

        if not paths:
            raise ValueError("paths must be an iterable of paths to back up")

        def to_directory_entry(directory_path):
            entry = DirectoryEntry.for_directory(directory_path)
            entry.write_cache()
            return entry

        file_entries = {}
        for path in paths:
            file_entries.update(cls._build_manifest(path, to_directory_entry, ignored_directories))

        return cls(cls.name_for(hostname, user), file_entries)

    @classmethod
    def _build_manifest(cls, path, f, ignored_directories=None):
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

        return { entry.path: entry for entry in itertools.chain.from_iterable(manifest_tree) }

    @classmethod
    def name_prefix_for(cls, hostname, user):
        return "manifest/{host}/{user}/".format(host=hostname, user=user)

    @classmethod
    def name_for(cls, hostname, user):
        return cls.name_prefix_for(hostname, user) + iso8601()
