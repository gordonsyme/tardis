import os
import errno
import hashlib
import datetime

def sha1sum(parts):
    """Create a sha1sum of each element in parts"""
    m = hashlib.sha1()
    for part in parts:
        m.update(part)
    return m.hexdigest()


def iso8601():
    """Get the current time as a string in ISO8601 format"""
    return datetime.datetime.utcnow().isoformat()


def makedirs(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
