import sys

from setuptools import setup, find_packages

from pycernan import __version__

if sys.version_info >= (3, 0):
    install_requires = ['avro-python3>=1.8.2']
else:
    install_requires = ['avro>=1.8.2']

setup(
    name="pycernan",
    version=__version__,
    author="John Koenig",
    author_email="john@postmates.com",
    description="Python client for Cernan.",
    license="MIT",
    keywords="client cernan",
    url="https://github.com/postmates/pycernan",
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-timeout',
        'mock>=1.0.1',
    ],
    install_requires=install_requires,
    include_package_data=True,
    scripts=[],
    classifiers=[
        "Topic :: Utilities",
    ],
)
