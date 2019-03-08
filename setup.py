from setuptools import setup, find_packages

from pycernan import __version__

install_requires = ['fastavro', 'future', 'prometheus_client']

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
        'pytz',
    ],
    install_requires=install_requires,
    include_package_data=True,
    scripts=[],
    classifiers=[
        "Topic :: Utilities",
    ],
)
