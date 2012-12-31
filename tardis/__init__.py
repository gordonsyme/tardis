import os
import os.path
import tempfile
import logging
import tarfile
import gzip
from contextlib import closing
from cStringIO import StringIO

from boto.s3.key import Key

from .util import iso8601, makedirs
from .manifest import Manifest


__temp_archive_name = "/tmp/tardis_temp.gz" # this is awful


def key_from(bucket, manifest_entry):
    key = Key(bucket)
    key.key = manifest_entry.object_id
    return key


def put_archive(bucket, create_archive, manifest_entry):
    archive_path = create_archive(manifest_entry.path)

    logging.debug("created archive {} for {}".format(archive_path, manifest_entry))

    with closing(key_from(bucket, manifest_entry)) as key:
        key.set_contents_from_filename(archive_path, encrypt_key=True)

    logging.debug("{} put successfully".format(manifest_entry))


def get_archive(bucket, manifest_entry):
    key = key_from(bucket, manifest_entry)
    key.get_contents_to_filename(__temp_archive_name)   # eww
    return __temp_archive_name                          # eww


def create_archive(path):
    logging.debug("Creating gzip for {}".format(path))

    with open(path, 'rb') as input_file:
        with gzip.open(__temp_archive_name, 'wb') as gzip_file:
            gzip_file.writelines(input_file)

    return gzip_file.name


def restore_archive(path, archive):
    logging.debug("Restoring from gzip {}".format(path))

    makedirs(os.path.dirname(path))

    with gzip.open(archive, 'rb') as gzip_file:
        with open(path, 'wb') as output_file:
            output_file.writelines(gzip_file)


def put_manifest(bucket, manifest):
    logging.debug("Putting manifest {}".format(manifest.name))

    with tempfile.NamedTemporaryFile(prefix="tmpmanifest", delete=False) as csvfile:
        manifest.to_csv(csvfile)
        manifest_filename = csvfile.name

    try:
        with closing(Key(bucket)) as key:
            key.key = manifest.name
            key.set_contents_from_filename(manifest_filename, encrypt_key=True)
    finally:
        os.unlink(manifest_filename)


def list_manifest_keys(bucket, hostname, user):
    key_prefix = Manifest.name_prefix_for(hostname, user)
    return [key for key in bucket.list(prefix=key_prefix) if not key.name == key_prefix]


def latest_manifest(bucket, hostname, user):
    keys = list_manifest_keys(bucket, hostname, user)

    if not keys:
        return Manifest({})

    most_recent_key = reversed(sorted(keys, key=lambda k: k.name)).next()

    with closing(StringIO(most_recent_key.get_contents_as_string())) as csvfile:
        manifest = Manifest.from_csv(csvfile, most_recent_key.name)

    logging.debug("Latest manifest - {}".format(manifest.name))

    return manifest


def needs_put(bucket, entry, new_entry):
    if entry.checksum_differs(new_entry) or new_entry.checksum_differs(entry): # eww
        logging.debug("Checksums differ")

        if not bucket.get_key(entry.object_id):
            logging.debug("Content not already archived")
            return True

    return False


def backup(backup_roots, skip_directories, put_archive, needs_put, put_manifest, get_manifest, create_manifest):
    latest_manifest = get_manifest()

    new_manifest = create_manifest(backup_roots, skip_directories)

    if new_manifest == latest_manifest:
        logging.debug("No content changed, nothing to backup")
        return

    for path in new_manifest:
        manifest_entry = new_manifest[path]

        if needs_put(manifest_entry, latest_manifest[path]):
            logging.debug("{} needs an update, putting to S3 {}".format(manifest_entry.path, manifest_entry.object_id))
            put_archive(manifest_entry)
        else:
            logging.debug("{} has not changed, not putting to S3".format(manifest_entry.path))

    put_manifest(new_manifest)


def restore(restore_roots, get_archive, restore_archive, get_manifest):
    manifest = get_manifest()

    for directory in restore_roots:
        path = os.path.abspath(directory)
        makedirs(path)

        logging.debug("Attempting to restore {}".format(path))

        paths = (f for f in manifest if f.startswith(path))

        for file_path in paths:
            logging.debug("Found {} in manifest".format(file_path))
            entry = manifest[file_path]
            logging.debug("Manifest entry is {}".format(entry))
            restore_archive(file_path, get_archive(entry))
