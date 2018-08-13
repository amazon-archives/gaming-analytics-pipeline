# coding: utf-8

from setuptools import setup, find_packages
try: #for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
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
