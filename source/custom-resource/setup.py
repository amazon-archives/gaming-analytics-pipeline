# coding: utf-8

from setuptools import setup, find_packages
from pip.req import parse_requirements

setup(
    name='custom_resource',
    version='1.0',
    description='Gaming Analytics Pipeline on AWS Custom Resource',
    author='AWS Solutions Builder',
    license='ASL',
    zip_safe=False,
    packages=['custom_resource'],
    package_dir={'custom_resource': '.'},
    include_package_data=False,
    install_requires=[
        'custom-resource>=1.0',
        'requests',
        'pg8000'
    ],
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
)
