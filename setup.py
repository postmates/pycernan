import os

from setuptools import setup, find_packages

setup(
    name="pycernan",
    version="0.0.1",
    author="John Koenig",
    author_email="john@postmates.com",
    description="Python client for Cernan.",
    license="MIT",
    keywords="client cernan",
    url="https://github.com/postmates/pycernan",
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-timeout',
        'mock>=1.0.1',
    ],
    install_requires=[
        'avro-python3',
    ],
    dependency_links=[
    ],
    include_package_data=True,
    scripts=[],
    classifiers=[
        "Topic :: Utilities",
    ],
)
