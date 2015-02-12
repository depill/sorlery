#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='sorlery',
    version='1.3.3',
    description="",
    author="Aidan Lister",
    author_email='aidan@aidanlister.com',
    url='https://github.com/aidanlister/sorlery',
    packages=find_packages(),
    install_requires=[
        'sorl_thumbnail',
        'celery',
    ],
)
