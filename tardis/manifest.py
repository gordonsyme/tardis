import os
import os.path
import logging
import csv
import functools
from collections import defaultdict

from tardis.util import sha1sum, iso8601


class ManifestEntry(object):
    """An entry in the backup manifest

    ManifestEntry objects maintain the relationship between a file name, the
    sha1sum of its content and the S3 object ID naming the content tarball.

    Note that the content tarball may not exist in S3 yet.
    """
    def __init__(self, file_path, object_id=None):
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

        if self.object_id is None:
            checksum = self._checksum_for_file(file_path)
            self.object_id = "data/{}/{}".format(sha1sum(os.path.basename(file_path)), checksum)

    def __repr__(self):
        return "<ManifestEntry({}, {})>".format(repr(self.path), repr(self.object_id))

    @property
    def checksum(self):
        return os.path.basename(self.object_id)

    def checksum_differs(self, other):
        return self.checksum != other.checksum

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
    def for_directory(cls, path):
        logging.debug("Creating manifest entries for {}".format(path))

        if not os.path.isdir(path):
            raise ValueError("{} does not name a directory".format(path))

        file_paths = filter(os.path.isfile, [os.path.join(path, f) for f in os.listdir(path)])

        if not file_paths:
            raise ValueError("{} contains no files".format(path))

        return (cls(file_path) for file_path in sorted(file_paths))

    @classmethod
    def from_fields(cls, fields):
        logging.debug("Creating ManifestEntry from: {}".format(fields))
        return cls(*fields)

    def as_fields(self):
        return (self.path, self.object_id)

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

    Maps file paths to the ManifestEntry for that file.

    Manifest keys are named in the following format:

    manifest/{hostname}/{user}/{timestamp}
    """
    def __init__(self, entries, name=None):
        self.entries = defaultdict(NullManifestEntry, entries)
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
        logging.debug("Writing {} to csv, entries: {}".format(self.__class__, self.entries))

        writer = csv.writer(stream, delimiter=':', lineterminator='\n')
        writer.writerow([self.name])
        writer.writerows(self.entries[entry].as_fields() for entry in self.entries)

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
    def for_directories(cls, hostname, user, directories, ignored_directories=None):
        if ignored_directories is None:
            ignored_directories = []

        entries = {}

        queue = [backup_root for backup_root in directories if os.path.isdir(backup_root)]

        for directory in queue:
            directory = os.path.abspath(directory)
            logging.debug("Examining {}".format(directory))

            if directory not in ignored_directories:
                try:
                    for entry in ManifestEntry.for_directory(directory):
                        entries[entry.path] = entry
                        logging.debug(entry)
                except ValueError as e:
                    logging.exception(e)

                # ewww
                subdirectories = [os.path.join(directory, subpath) for subpath in os.listdir(directory)]
                subdirectories = [subdir for subdir in subdirectories if os.path.isdir(subdir)]

                queue.extend(sorted(subdirectories))

        return cls(entries, cls.name_for(hostname, user))
