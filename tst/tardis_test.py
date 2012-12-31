from collections import namedtuple

from nose.tools import *
from mock import Mock

from tardis import list_manifest_keys


MockManifestKey = namedtuple("MockManifestKey", ['name'])


manifest_names = [ MockManifestKey("manifest/hostname/username/")
                 , MockManifestKey("manifest/hostname/username/2012-11-25T12:01:00.000000")
                 , MockManifestKey("manifest/hostname/username/2012-11-25T12:05:00.000000")
                 , MockManifestKey("manifest/hostname/username/2012-11-25T12:02:00.000000")
                 ]

def test_list_manifest_keys():
    bucket = Mock()
    bucket.list.return_value = manifest_names

    expected = [ MockManifestKey("manifest/hostname/username/2012-11-25T12:01:00.000000")
               , MockManifestKey("manifest/hostname/username/2012-11-25T12:05:00.000000")
               , MockManifestKey("manifest/hostname/username/2012-11-25T12:02:00.000000")
               ]

    assert_equals(expected, list_manifest_keys(bucket, 'hostname', 'username'))
