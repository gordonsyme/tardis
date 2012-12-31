#!/usr/bin/env python

from distutils.core import setup

setup(
        name='tardis',
        version='1.0',
        description='Hacky backup to S3',
        author='Gordon Syme',
        author_email='gordonsyme@gmail.com',
        packages=['tardis'],
        scripts=['archive']
     )
