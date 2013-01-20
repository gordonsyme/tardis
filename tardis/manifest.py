import os
import os.path
import pwd, grp, stat

import logging
import csv
import functools
from collections import defaultdict

from tardis.util import sha1sum, iso8601


class ManifestEntry(object):
    """An entry in the backup manifest

    ManifestEntry objects maintain the relationship between a file name, the
    sha1sum of its content and the S3 object ID naming the content.

    TODO insert comment about stat info        

    Note that the content may not exist in S3 yet.
    """
    def __init__(self, file_path, object_id=None, *stat_info):
        """Create a Manifest

        file_path - file name
        checksum - checksum of the file's content.
        object_id - S3 object id for the content tarball. If ommitted a unique
                    id is generated with the "data/" prefix.
        """
        logging.debug("Creating manifest entry for {}".format(file_path))

        if not file_path:
            raise ValueError("Must specify a file path")

        self.path = file_path
        self.object_id = object_id
        self.stat_info = stat_info

    def __repr__(self):
        return "<ManifestEntry({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})>".format(
                        self.path, self.object_id, *self.stat_info)

    @property
    def checksum(self):
        return os.path.basename(self.object_id)

    def checksum_differs(self, other):
        return self.checksum != other.checksum

    @classmethod
    def from_fields(cls, fields):
        logging.debug("Creating ManifestEntry from: {}".format(fields))
        return cls(*fields)

    def as_fields(self, base_path=''):
        return (os.path.join(base_path, self.path), self.object_id) + self.stat_info

    def __eq__(self, other):
        if isinstance(other, ManifestEntry):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result


class NullManifestEntry(object):
    """A ManifestEntry that is always different to every other"""
    def __init__(self):
        self.checksum = ""

    def checksum_differs(self, other):
        return True


class Manifest(object):
    """A backup manifest.

    <
    Recursive structure:
        - current directory
        - ManifestEntries for each regular file
        - Manifests for each subdirectory
    >

    Maps file paths to the ManifestEntry for that file.

    Manifest keys are named in the following format:

    manifest/{hostname}/{user}/{timestamp}
    """
    def __init__(self, path, entries=None, manifests=None, name=None):
        self.path = path
        self.manifests = manifests
        self.entries = entries
        self._name = name

    @property
    def name(self):
        if self._name is None:
            return "unnamed"
        return self._name

    def __getitem__(self, item):
        return self.entries.__getitem__(item)

    def __contains__(self, item):
        return self.entries.__contains__(item)

    def __iter__(self):
        return self.entries.__iter__()

    def __eq__(self, other):
        if isinstance(other, Manifest):
            return self.entries == other.entries
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def to_csv(self, stream):
        # Write the name of *this* manifest, but not the sub-manifests
        # Each entry needs to know the path in order to properly write filenames
        logging.debug("Writing {} to csv, entries: {}".format(self.__class__, self.entries))

        writer = csv.writer(stream, delimiter=':', lineterminator='\n')
        writer.writerow([self.name])
        self._to_csv(writer)

    def _to_csv(self, writer):
        writer.writerows(entry.as_fields(self.path) for entry in self.entries)
        for submanifest in manifests:
            submanifest._to_csv(writer)

    @classmethod
    def name_prefix_for(cls, hostname, user):
        return "manifest/{host}/{user}/".format(host=hostname, user=user)

    @classmethod
    def name_for(cls, hostname, user):
        return cls.name_prefix_for(hostname, user) + iso8601()

    @classmethod
    def from_csv(cls, stream, name=None):
        logging.debug("Creating {} from csv".format(cls))

        reader = csv.reader(stream, delimiter=':')
        name = reader.next()

        entries = [ManifestEntry.from_fields(fields) for fields in reader]

        return cls({ entry.path : entry for entry in entries }, name)

    @classmethod
    def for_directory(cls, hostname, user, path, ignored_directories=None):
        if not hostname:
            raise ValueError("hostname must be a non-empty string")

        if not user:
            raise ValueError("user must be a non-empty string")

        if not os.path.isdir(path):
            raise ValueError("{} does not name a directory".format(path))

        if not ignored_directories:
            ignored_directories = []

        path = os.path.abspath(path)

        logging.debug("Creating manifest for {}".format(path))

        if path not in ignored_directories:
            paths = [ os.path.join(path, p) for p in os.listdir(path) ]

            entries = [ ManifestEntry(os.path.basename(f), cls._object_id(f), *cls._stat_info(f))
                            for f in paths if os.path.isfile(f) ]

            manifests = [ cls.for_directory(hostname, user, d, ignored_directories)
                            for d in paths if os.path.isdir(d) ]

            return cls(path, entries, manifests, cls.name_for(hostname, user))

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

    @classmethod
    def _stat_info(cls, path):
        stat_struct = os.stat(path)

        owner = pwd.getpwuid(stat_struct.st_uid).pw_name
        group = grp.getgrgid(stat_struct.st_gid).gr_name
        mode  = stat.S_IMODE(stat_struct.st_mode)

        ctime = os.path.getctime(path)
        mtime = os.path.getmtime(path)
        size  = os.path.getsize(path)

        return (owner, group, mode, int(ctime), int(mtime), size)
